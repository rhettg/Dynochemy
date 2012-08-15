# -*- coding: utf-8 -*-
"""
This module contains classes for abstract Dynochemy operations.

This abstraction is a framework for combining multiple operations into sets of
operations that can be exectuted together.

A key part of the interface is to use 'reduce' to combine operations together.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import functools

from . import errors
from . import defer
from . import utils
from . import db as db_mod

class ReduceError(errors.Error): pass


def reduce_operations(left_op, right_op):
    try:
        return left_op.reduce(right_op)
    except ValueError:
        try:
            return right_op.reduce(left_op)
        except ValueError:
            raise ReduceError("No path for %s to %s", left_op.__class__.__name__, right_op.__class__.__name__)


class Operation(object):
    #def reduce(self, op):
        #raise NotImplementedError

    def run_defer(self, db):
        raise NotImplementedError

    @property
    def unique_key(self):
        raise NotImplementedError

    def __eq__(self, other):
        return self.unique_key == other.unique_key

    def __hash__(self):
        return hash(self.unique_key)

    def run(self, db):
        df = self.run_defer(db)
        result, err = df()
        if err:
            raise err
        return result

    def run_async(self, db, callback=None):
        df = self.run_defer(db)
        def handle_result(cb):
            if callback is not None:
                callback(*cb.result)

        df.add_callback(handle_result)



class OperationSet(Operation):
    """Operation that does multiple sub-operations"""
    def __init__(self):
        # Really we can combine everything into into two operations
        self.update_ops = []
        self.batch_write_op = BatchWriteOperation()
        self.batch_read_op = BatchReadOperation()

    def reduce(self, op):
        op_set = OperationSet()

        for update in self.update_ops:
            op_set.add_update(update)

        op_set.batch_write_op = self.batch_write_op
        op_set.batch_read_op = self.batch_read_op

        if isinstance(op, OperationSet):
            for update_op in op.update_ops:
                op_set.add_update(update_op)

            op_set.batch_write_op = op_set.batch_write_op.reduce(op.batch_write_op)

        elif isinstance(op, _WriteBatchableMixin):
            op_set.batch_write_op = self.batch_write_op.reduce(op)

        elif isinstance(op, _ReadBatchableMixin):
            op_set.batch_read_op = self.batch_read_op.reduce(op)

        elif isinstance(op, UpdateOperation):
            op_set.add_update(op)

        else:
            raise ValueError(op)

        return op_set

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

    def run_defer(self, db):
        # This one is a little more complicated. We have to run all
        # sub-operations and then combine the OperationResult objects

        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        batch_write_defer = None
        update_defers = []

        # This result handler is going to track all our results. When they are done, we complete the master defer object
        # and report the results
        def record_result(cb):
            r, err = cb.result

            result.update(r)

            if batch_write_defer and batch_write_defer.done and all(df.done for df in update_defers):
                # This guy takes a defer itself, we'll just use the last one, but it shouldn't really matter
                # Error handling is weird at this level.
                df.callback(cb)

        for op in self.update_ops:
            op_defer = op.run_defer(db)
            update_defers.append(op_defer)
            op_defer.add_callback(record_result)

        # note: we're doing this last because in sync mode, stuff is always
        # done and we don't want to trigger the callback till we have created
        # all the defers.
        batch_write_defer = self.batch_write_op.run_defer(db)
        batch_write_defer.add_callback(record_result)

        return df


def combine_dicts(left_dict, right_dict, combiner=None):
    """Utility function for combining two dictionaries (union) with a user specified 'combiner' function

    Note that the default combiner just takes the value in right first, followed by left if they are truthy
    """
    if combiner is None:
        def combiner(l, r):
            return r or l

    if not any((left_dict, right_dict)):
        return {}

    if not left_dict and right_dict:
        # Swap arguments so we have some value
        s_dict = left_dict
        left_dict = right_dict
        right_dict = {}

    right_dict = right_dict or {}

    out_dict = left_dict.copy()
    for k, v in right_dict.iteritems():
        out_dict[k] = combiner(left_dict.get(k), v)
    return out_dict


class UpdateOperation(Operation):
    def __init__(self, table, key, add=None, put=None, delete=None):
        self.table = table
        self.key = key
        self.add = add
        self.put = put
        self.delete = delete

    def reduce(self, op):
        if isinstance(op, UpdateOperation):
            op_set = OperationSet()
            op_set.update_ops.append(self)
            op_set.update_ops.append(op)
            return op_set
        else:
            raise ValueError(op)

    def combine_updates(self, update_op):
        """Combine two UpdateOperations assuming they are for the same table/key combination"""
        assert self.table == update_op.table
        assert self.key == update_op.key

        new_update = UpdateOperation(self.table, self.key)

        def add_combiner(l, r):
            if l and r:
                return l + r
            else:
                return l or r

        new_update.add = combine_dicts(self.add, update_op.add, combiner=add_combiner)
        new_update.put = combine_dicts(self.put, update_op.put)
        new_update.delete = combine_dicts(self.delete, update_op.delete)

        return new_update

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, cb.result)
            df.callback(cb)

        update_df = getattr(db, self.table.__name__).update_defer(self.key, add=self.add, put=self.put, delete=self.delete)
        update_df.add_callback(record_result)

        return df

    @property
    def unique_key(self):
        return ('UPDATE', self.table.name, self.key)


class _WriteBatchableMixin(object):
    """Mixing for operations that can be put in a batch write"""
    def reduce(self, op):
        batch_op = BatchWriteOperation()
        batch_op.add(self)
        batch_op.add(op)
        return batch_op

class _ReadBatchableMixin(object):
    """Mixing for operations that can be put in a batch read"""
    def reduce(self, op):
        batch_op = BatchReadOperation()
        batch_op.add(self)
        batch_op.add(op)
        return batch_op


class BatchWriteOperation(Operation, _WriteBatchableMixin):
    __slots__ = ["ops"]
    def __init__(self):
        self.ops = []

    def add(self, op):
        if isinstance(op, BatchWriteOperation):
            self.ops += op.ops
        else:
            self.ops.append(op)

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)
        if not self.ops:
            df.done = True
            return df

        def record_result(op, cb):
            result.record_result(op, cb.result)

        all_batch_defers = []
        def handle_batch_result(cb):
            if all(df.done for df in all_batch_defers) and not df.done:
                df.callback(cb)

        for op_set in utils.segment(self.ops, db_mod.WriteBatch.MAX_ITEMS):
            batch = db.batch_write()
            for op in op_set:
                op_df = op.add_to_batch(batch)
                op_df.add_callback(functools.partial(record_result, op))

            batch_df = batch.defer()
            all_batch_defers.append(batch_df)

        for batch_df in all_batch_defers:
            batch_df.add_callback(df.callback)

        return df


class BatchReadOperation(Operation, _ReadBatchableMixin):
    __slots__ = ["ops"]
    def __init__(self):
        self.ops = []

    def add(self, op):
        if isinstance(op, BatchReadOperation):
            self.ops += op.ops
        else:
            self.ops.append(op)

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        if not self.ops:
            df.done = True
            return df

        def record_result(op, cb):
            result.record_result(op, cb.result)

        all_batch_defers = []
        def handle_batch_result(cb):
            if all(df.done for df in all_batch_defers) and not df.done:
                df.callback(cb)

        for op_set in utils.segment(self.ops, db_mod.ReadBatch.MAX_ITEMS):
            batch = db.batch_read()
            for op in op_set:
                op_df = op.add_to_batch(batch)
                op_df.add_callback(functools.partial(record_result, op))

            batch_df = batch.defer()
            all_batch_defers.append(batch_df)

        for batch_df in all_batch_defers:
            batch_df.add_callback(handle_batch_result)

        return df

class PutOperation(Operation, _WriteBatchableMixin):
    def __init__(self, table, entity):
        self.table = table
        self.entity = entity

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, cb.result)
            df.callback(cb)

        op_df = getattr(db, self.table.__name__).put_defer(self.entity)
        op_df.add_callback(record_result)

        return df

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).put(self.entity)

    @property
    def unique_key(self):
        key = tuple([self.entity[k] for k in self.table(None).key_spec])
        return ('PUT', self.table.name, key)


class DeleteOperation(Operation, _WriteBatchableMixin):
    def __init__(self, table, key):
        self.table = table
        self.key = key

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, cb.result)
            df.callback(cb)

        op_df = getattr(db, self.table.__name__).delete_defer(self.key)
        op_df.add_callback(record_result)

        return df

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).delete(self.key)

    @property
    def unique_key(self):
        # Assumes updates for the same key have been combined together
        return ('DELETE', self.table.name, self.key)


class GetOperation(Operation, _ReadBatchableMixin):
    def __init__(self, table, key):
        self.table = table
        self.key = key

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, cb.result)
            df.callback(cb)

        op_df = getattr(db, self.table.__name__).get_defer(self.key)
        op_df.add_callback(record_result)

        return df

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).get(self.key)

    @property
    def unique_key(self):
        # Assumes updates for the same key have been combined together
        return ('GET', self.table.name, self.key)


class OperationResultDefer(defer.Defer):
    """Special defer that returns a OperationResult, error tuple
    """
    def __init__(self, op_result, io_loop):
        super(OperationResultDefer, self).__init__(io_loop)
        self.op_result = op_result
        self.error = None

    def callback(self, cb):
        # We are always called via another callback
        assert cb.done
        _, err = cb.result
        if err:
            self.error = err

        super(OperationResultDefer, self).callback(cb)
        
    @property
    def result(self):
        return self.op_result, self.error


# TODO: Make this just a dict?
class OperationResult(object):
    def __init__(self, db):
        self.db = db
        self.results = {}

    def record_result(self, op, result):
        self.results[op] = result

    def update(self, other_result):
        assert self.db == other_result.db
        self.results.update(other_result.results)

    def iteritems(self):
        return self.results.iteritems()

    def __getitem__(self, key):
        return self.results[key]

    def __repr__(self):
        return repr(self.results)

