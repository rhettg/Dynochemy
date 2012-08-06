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
        self.update_ops.append(update_op)
        return

        # TODO: We should be able to combine updates
        by_key = collections.defaultdict(list)
        for op in ops + [self]:
            if isinstance(op, UpdateOperation):
                update_ops = by_key[(op.table, op.key)]
                if 'add' in update_ops:
                    for k, v in update_ops['add'].iteritems():
                        if k in update_ops['add']:
                            update_ops['add'][k] += v
                        else:
                            update_ops['add'][k] = v
                else:
                    update_ops[add] = op.add.copy()

                update_ops.setdefault('put', {}).update(op.put)
                update_ops.setdefault('delete', {}).update(op.delete)
            else:
                new_op = OperationSet.reduce([new_op])

    def run(self, db):
        self.batch_write_op.run(db)
        for op in self.update_ops:
            op.run(db)


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

