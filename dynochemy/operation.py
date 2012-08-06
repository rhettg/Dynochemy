# -*- coding: utf-8 -*-
"""
This module contains classes for abstract Dynochemy operations.

This abstraction is a framework for combining multiple operations into sets of
operations that can be exectuted together.

A key part of the interface is to use 'reduce' to combine operations together.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
from dynochemy import errors

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

    def run(self, db):
        raise NotImplementedError

    def run_async(self, db, callback=None):
        raise NotImplementedError

    def run_defer(self, db):
        raise NotImplementedError


class OperationSet(Operation):
    """Operation that does multiple sub-operations"""
    def __init__(self):
        # Really we can combine everything into into two operations
        self.update_ops = []
        self.batch_write_op = BatchWriteOperation()

    def reduce(self, op):
        op_set = OperationSet()

        for update in self.update_ops:
            op_set.add_update(update)

        op_set.batch_write_op = self.batch_write_op

        if isinstance(op, OperationSet):
            for update_op in op.update_ops:
                op_set.add_update(update_op)

            op_set.batch_write_op = op_set.batch_write_op.reduce(op.batch_write_op)

        elif isinstance(op, _BatchableMixin):
            op_set.batch_write_op = self.batch_write_op.reduce(op)

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

    def run(self, db):
        self.batch_write_op.run(db)
        for op in self.update_ops:
            op.run(db)


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

    def run(self, db):
        return getattr(db, self.table.__name__).update(self.key, add=self.add, put=self.put, delete=self.delete)


class _BatchableMixin(object):
    """Mixing for operations that can be put in a batch write"""
    def reduce(self, op):
        batch_op = BatchWriteOperation()
        batch_op.add(self)
        batch_op.add(op)
        return batch_op


class BatchWriteOperation(Operation, _BatchableMixin):
    __slots__ = ["ops"]
    def __init__(self):
        self.ops = []

    def run(self, db):
        batch = db.batch_write()
        for op in self.ops:
            op.add_to_batch(batch)

        return batch()

    def add(self, op):
        if isinstance(op, BatchWriteOperation):
            self.ops += op.ops
        else:
            self.ops.append(op)


class PutOperation(Operation, _BatchableMixin):
    def __init__(self, table, entity):
        self.table = table
        self.entity = entity

    def run(self, db):
        return getattr(db, self.table.__name__).put(self.entity)

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).put(self.entity)


class DeleteOperation(Operation, _BatchableMixin):
    def __init__(self, table, key):
        self.table = table
        self.key = key

    def run(self, db):
        return getattr(db, self.table.__name__).delete(self.key)

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).delete(self.key)

