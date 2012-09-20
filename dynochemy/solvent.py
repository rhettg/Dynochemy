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
        self.op_seq = [operation.OperationSet()]

    def add_operation(self, op):
        self.op_seq[0].add(op)

    def put(self, table, entity):
        table_cls = classify(table)
        op = operation.PutOperation(table_cls, entity)
        self.add_operation(op)
        return op

    def delete(self, table, key):
        table_cls = classify(table)
        op = operation.DeleteOperation(table_cls, key)
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

    def query(self, table):
        raise NotImplementedError

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
        # This is a tough little chunk of code. Not really clear how to
        # refactor to make easier, so I'll try to write a quick overview.
        # Basically, our entire solvent is running under a single defer object
        # ('final_df'), which will be called back with the entire defer is
        # complete. The argument to that callback will be a 'OperationResult'
        # object which provides a mapping from operation to result.

        # During each attempt inside this defer, we'll take all our operations,
        # combine them together into a single operation and run it.  If, after
        # processing the results of that operation (in handle_result), there
        # are additional operations to run, or operations which need to be
        # tried again due to certain failures, we'll do another loop if it.
        final_results = operation.OperationResult(db)
        final_df = operation.OperationResultDefer(final_results, db.ioloop)

        # Ideally, a solvent can be run multiple times against different databases.
        # To keep our op_sequence un-molested, we'll create a copy before doing any
        # view operations
        op_sequence = view.view_operations(db, self.op_seq)

        # Note: our attempts counter exists outside the closure, and do to
        # scoping rules, if we want a counter we need to put it in an array so
        # it persists. I know it's weird, but go ahead and try it.

        # How many attempts (usually due to a failure)
        attempts = [0]

        # How many actual iterations have we gone through
        steps = [0]

        def handle_result(cb):
            remaining_ops = operation.OperationSet()
            results, err = cb.result
            for op, (r, err) in results.iteritems():
                # Certain types of errors we know we can try again just be re-executing the same operation.
                # Other errors, or successes, we'll just record and carry on.
                if isinstance(err, (errors.ProvisionedThroughputError, errors.UnprocessedItemError)):
                    log.debug("Provisioning error for %r on table %r", op, op.table.name)
                    remaining_ops.add(op)
                else:
                    # It's possible that upon recording results, an operation
                    # may have a follow-up operation. So we'll add taht to our
                    # remaining ops.
                    next_ops = final_results.record_result(op, (r, err))
                    if next_ops:
                        # We have more operations. Create or add these operations to the next
                        # step in the sequence.
                        if len(op_sequence) <= 1:
                            op_sequence.append(operation.OperationSet())
                        for op in next_ops:
                            op_sequence[1].add(op)

            # We've updated individual results piecemeal, but we're going to
            # need our capacity values as well.
            final_results.update_write_capacity(results.write_capacity)
            final_results.update_read_capacity(results.read_capacity)

            if remaining_ops and attempts[0] < MAX_ATTEMPTS:
                log.debug("%d remaining operations after attempt %d", len(remaining_ops), attempts[0])

                # We need to queue another operation
                def run_remaining_ops():
                    next_df = remaining_ops.run_defer(db)
                    next_df.add_callback(handle_result)

                # We only track attempts if we are doing more because of failures.
                attempts[0] += 1
                delay_secs = 0.8 * attempts[0]
                log.debug("Trying again in %.1f seconds", delay_secs)

                if db.ioloop:
                    db.ioloop.add_timeout(time.time() + delay_secs, run_remaining_ops)
                else:
                    # No ioloop, do it inline
                    time.sleep(delay_secs)
                    run_remaining_ops()

            elif remaining_ops:
                log.warning("Gave up after %d attempts", attempts[0])
                final_df.callback(cb)
            else:
                log.debug("Step complete")
                attempts[0] = 0
                op_sequence.pop(0)

                if len(op_sequence) > 0:
                    log.debug("Starting next step in sequence, %d operations", len(op_sequence[0]))
                    next_df = op_sequence[0].run_defer(db)
                    next_df.add_callback(handle_result)
                else:
                    log.debug("Solvent complete")
                    # We're all done, complete the final df
                    final_df.callback(cb)

        op_df = op_sequence[0].run_defer(db)
        op_df.add_callback(handle_result)
        return final_df


class Sequence(object):
    """object to manage sets of operations which should be executed in order."""

    def __init__(self, op_set=None):
        self.operation_sets = op_set or []
        self.ndx = 0

    @property
    def current(self):
        """Returns the current operation set"""
        if len(self.operation_sets) == 0:
            op_set = operation.OperationSet()
            self.operation_sets.append(op_set)

        return self.operation_sets[0]

    @property
    def previous(self):
        """Returns an operation before the current"""
        op_set = operation.OperationSet()
        self.operation_sets.insert(0, op_set)
        return op_set

    @property
    def next(self):
        """Returns the next operation set"""
        if len(self.operation_sets) > 1:
            return self.operation_sets[1]
        else:
            op_set = operation.OperationSet()
            self.operation_sets.append(op_set)
            return op_set

    def pop(self):
        """Remove the current set, as it must be complete"""
        return self.operation_sets.pop(0)

    def __len__(self):
        return len(self.operation_sets)


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
