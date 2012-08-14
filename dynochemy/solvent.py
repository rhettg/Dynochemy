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

from . import operation

class Solvent(object):
    """A solvent is a abstraction over Dynochemy database operations where
    operations can be combined together and executed with some intelligence.

    This includes:
        * automatic secondary index maintenance
        * throttling
        * memcache write-through caches and invalidation
    """
    def __init__(self):
        self.operations = []

    def _op(self):
        op = reduce(operation.reduce_operations, self.operations)
        return op

    def put(self, table, entity):
        op = operation.PutOperation(table, entity)
        self.operations.append(op)
        return op

    def delete(self, table, key):
        op = operation.DeleteOperation(table, key)
        self.operations.append(op)
        return op

    def update(self, table, key, put=None, add=None, delete=None):
        op = operation.UpdateOperation(table, key, put=put, add=add, delete=delete)
        self.operations.append(op)
        return op

    def get(self, table, key):
        op = operation.GetOperation(table, key)
        self.operations.append(op)
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
        final_results = operation.OperationResult(db)
        final_df = operation.OperationResultDefer(final_results, db.ioloop)

        def handle_result(cb):
            remaining_ops = []
            results, err = cb.result
            for op, (r, err) in results.iteritems():
                if err:
                    remaining_ops.append(op)
                else:
                    final_results.record_result(op, (r, err))

            if remaining_ops:
                # We need to queue another operation
                def run_next_ops():
                    next_df = reduce(operation.reduce_operations, remaining_ops).run_defer(db)
                    next_df.add_callback(handle_result)

                if db.ioloop:
                    db.ioloop.add_timeout(time.time() + 0.5, run_next_ops)
                else:
                    time.sleep(0.5)
                    run_next_ops()
            else:
                # We're all done, complete the final df
                final_df.callback(op_df)

        op_df = self._op().run_defer(db)
        op_df.add_callback(handle_result)
        return final_df


if __name__ == '__main__':

    TestSolvent.register_table(TestTable)

    s = TestSolvent()

    put_op_1 = s.put(TestTable, {'key': 'hello', 'value': 10})
    put_op_2 = s.put(TestTable, {'key': 'world', 'value': 25.2})

    result = s.run(db)

    print result[put_op_1]
    print result[put_op_2]
