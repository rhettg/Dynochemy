# -*- coding: utf-8 -*-
"""
This module contains the Solvent system which allows for higher level database operations
on a DynamoDB data store.

Usage is generally to create a instance of Solvent() class, configuring it with a set of operations,
and then running it.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import time
import logging
import copy
import types

from . import operation
from . import view
from . import errors
from . import db

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 5

def classify(table_or_cls):
    if table_or_cls.__class__ == type:
        return table_or_cls
    else:
        return table_or_cls.__class__

class Solvent(object):
    """A solvent is a abstraction over Dynochemy database operations where
    operations can be combined together and executed with some intelligence.

    This includes:
        * automatic secondary index maintenance
        * throttling
        * memcache write-through caches and invalidation
    """
    def __init__(self):
        # We always start with an operation set, because everything can be reduced into it.
        self.operations = []

    def add_operation(self, op):
        self.operations.append(op)

    def __len__(self):
        return len(self.operations)

    def put(self, table, entity):
        table_cls = classify(table)
        op = operation.PutOperation(table_cls, entity)
        self.add_operation(op)
        return op

    def delete(self, table, key):
        table_cls = classify(table)
        op = operation.GetAndDeleteOperation(table_cls, key)
        self.add_operation(op)
        return op

    def update(self, table, key, put=None, add=None, delete=None):
        table_cls = classify(table)
        op = operation.UpdateOperation(table_cls, key, put=put, add=add, delete=delete)
        self.add_operation(op)
        return op

    def get(self, table, key):
        table_cls = classify(table)
        op = operation.GetOperation(table_cls, key)
        self.add_operation(op)
        return op

    def query(self, table_or_view, key):
        table_or_view_cls = classify(table_or_view)
        if issubclass(table_or_view_cls, db.Table):
            op = operation.QueryOperation(table_or_view_cls, key)
            self.add_operation(op)
        else:
            op = table_or_view_cls.query_op(key)
            self.add_operation(op)

        return op

    def scan(self, table):
        raise NotImplementedError

    def run(self, db):
        df = self.run_defer(db)
        result, err = df()
        if err:
            raise err
        return result

    def run_async(self, db, callback=None):
        def handle_result(cb):
            if callback is not None:
                callback(*cb.result)
        df = self.run_defer(db)
        df.add_callback(handle_result)

    def run_defer(self, db):
        run = SolventRun(db, self.operations)
        return run.run_defer()


class SolventRun(object):
    def __init__(self, db, ops):
        self.db = db

        self.operations = copy.copy(ops)

        self.op_results = operation.OperationResult(db)
        self.op_results.add_callback(self.handle_result)

        self.defer = operation.OperationResultDefer(self.op_results, db.ioloop)

        self.current_op_dfs = []

    def add_operation(self, op):
        self.operations.append(op)

    def run_defer(self):
        self.next_step()

        return self.defer

    def handle_result(self, op):
        result, err = self.op_results[op]
        if err:
            # Not going to do anything with errors
            return

        # This provides a hook for each operation that completes. We have the opportunity to
        # do special stuff like interact with views based on the results of an operation.
        for view in self.db.views_by_table(op.table):
            next_ops = view.operations_for_operation(op, result)
            if next_ops:
                log.info("Found %d view operations for %r", len(next_ops), op)

            for op in next_ops:
                self.add_operation(op)

    def next_step(self):
        next_ops = []
        # Start our set of operations off with whatever our results object says we have to do.
        # These are usually complex operations with multiple stages.
        while self.op_results.next_ops:
            next_ops.append(self.op_results.next_ops.pop(0))

        # No ops left over from last time, grab the next part of the sequence.
        if not next_ops and self.operations:
            next_ops += self.operations
            del self.operations[:]

        if not next_ops:
            # All done, no more operations to complete
            self.defer.callback(None)
            return

        for op in OperationSet(next_ops).ops:
            op_df = op.run_defer(self.op_results)
            self.current_op_dfs.append(op_df)

        # We add our callbacks all at once, because in sync mode, we want to
        # ensure all our op_dfs have been created before we start to look for
        # finished ones.
        for op_df in self.current_op_dfs:
            # After each op, we also need to check for completion of all our
            # ops (based on the all_op_dfs list itself)
            op_df.add_callback(self.check_step_done)


    def check_step_done(self, cb):
        """Check if our solvent has completed a set of operations and handle errors"""
        if self.current_op_dfs and all(df.done for df in self.current_op_dfs):

            # We have a current list, and they are all done. So this guy is complete.
            del self.current_op_dfs[:]

            has_failed_ops = self.requeue_failed_ops()
            if has_failed_ops:
                delay_secs = 0.8 * self.op_results.error_attempts
                log.debug("Trying again in %.1f seconds", delay_secs)

                db = self.op_results.db
                if db.ioloop:
                    db.ioloop.add_timeout(time.time() + delay_secs, self.next_step)
                else:
                    # No ioloop, do it inline
                    time.sleep(delay_secs)
                    self.next_step()
            else:
                self.next_step()

    def requeue_failed_ops(self):
        has_failures = False

        # We'll only requeue failed operations a fixed number of times.
        if self.op_results.error_attempts < MAX_ATTEMPTS:

            self.op_results.error_attempts += 1

            # Check for errors and do some retries.
            for op in self.op_results:
                _, err = self.op_results[op]

                # Certain types of errors we know we can try again just be re-executing the same operation.
                # Other errors, or successes, we'll just record and carry on.
                if isinstance(err, (errors.ProvisionedThroughputError, errors.UnprocessedItemError)):
                    log.debug("Provisioning error for %r on table %r", op, op.table.name)
                    self.add_operation(op)
                    has_failures = True

        return has_failures


class OperationSet(object):
    """Object for combining multiple operations together intelligently.

    Individual operations such as Put and Get and some Updates can be combined together into batches.
    """
    def __init__(self, operation_list=None):
        # Really we can combine everything into into two operations
        self.update_ops = []
        self.other_ops = []
        self.batch_write_op = operation.BatchWriteOperation()
        self.batch_read_op = operation.BatchReadOperation()

        if operation_list:
            for op in operation_list:
                self.add(op)

    def __len__(self):
        return len(self.update_ops) + len(self.other_ops) + len(self.batch_write_op) + len(self.batch_read_op)

    def __copy__(self):
        return OperationSet(list(self))

    def __iter__(self):
        for op in itertools.chain(self.update_ops, self.other_ops, self.batch_write_op, self.batch_read_op):
            yield op

    @property
    def ops(self):
        ops = []
        ops += self.update_ops
        ops += self.other_ops
        if len(self.batch_write_op) > 0:
            ops.append(self.batch_write_op)
        if len(self.batch_read_op) > 0:
            ops.append(self.batch_read_op)

        return ops

    def add(self, op):
        if isinstance(op, operation._WriteBatchableMixin):
            self.batch_write_op.add(op)
        elif isinstance(op, operation._ReadBatchableMixin):
            self.batch_read_op.add(op)
        elif isinstance(op, operation.UpdateOperation):
            self.add_update(op)
        elif isinstance(op, operation.QueryOperation):
            self.other_ops.append(op)
        else:
            raise ValueError("Don't know how to add op %r" % op)

    def add_update(self, update_op):
        # We could keep just a list of all updates and then execute them
        # sequentially, but updates might be easily combined if they are for the same
        # table/key pair

        combined_update_op = False
        new_update_ops = []
        for op in self.update_ops:
            if not combined_update_op and op.table == update_op.table and op.key == update_op.key:
                # We can combine these updates
                new_update_ops.append(op.combine_updates(update_op))
                combined_update_op = True
            else:
                new_update_ops.append(op)

        if not combined_update_op:
            new_update_ops.append(update_op)

        self.update_ops = new_update_ops
        return


if __name__ == '__main__':

    class TestTable(Table):
        name = 'test_table'
        hash_key = 'key'

    db.register(TestTable)

    s = TestSolvent()

    put_op_1 = s.put(TestTable, {'key': 'hello', 'value': 10})
    put_op_2 = s.put(TestTable, {'key': 'world', 'value': 25.2})

    result = s.run(db)

    print result[put_op_1]
    print result[put_op_2]
