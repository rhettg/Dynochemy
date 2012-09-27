# -*- coding: utf-8 -*-
"""
This module contains objects for handling creating views in Dynochemy.

A view manages a table, populated from operations in another table. You can
think of this as indexing, but rather than just indexing entities, you can
create any kind of table you want. For example, you can have counts of entities
with certain values.

The only real requirement is that a view get registered for updates to a
specific table. It can then return additional operations that should be
included in the same batch of updates.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import copy

from . import operation

def view_operations(db, op_sequence):
    """Given the 'db' as context, find all our views and generate additional
    operations that should result from the user supplied operation
    """

    # The order of operations is important. We allow views to define operations
    # that should happen before the primary operation so you can do things like
    # get the current value before deleting it.

    # Otherwise, view operations should happen AFTER the primary operation has
    # completed. Otherwise we open ourselves up to the possibility that the
    # index operation could complete, while the primary operation has not.
    # While we can't guarantee consistency with this type of datastore, we can
    # at least reduce the chances. It's worth the possible performance penalty.
    new_op_seq = []
    for op_set in op_sequence:
        seq_prev_ops = []
        seq_current_ops = []
        seq_next_ops = []

        for op in op_set:
            seq_current_ops.append(op)

            for view in db.views_by_table(op.table):
                prev_ops, next_ops = view.operations_for_operation(op)
                seq_prev_ops += prev_ops
                seq_next_ops += next_ops

        if seq_prev_ops:
            new_op_seq.append(seq_prev_ops)
        if seq_current_ops:
            new_op_seq.append(seq_current_ops)
        if seq_next_ops:
            new_op_seq.append(seq_next_ops)

    return new_op_seq


class GetAndRemoveOperation(operation.GetOperation):
    """To remove an entity from a view, you really need to know what the entity is.

    This operation is a sequence that first retrieves the entity to be removed and then
    passes it along to the view's remove() call so it can generate it's own operation.
    """
    def __init__(self, table, key, view):
        super(GetAndRemoveOperation, self).__init__(table, key)
        self.view = view

    def have_result(self, op_results, op_cb):
        super(GetAndRemoveOperation, self).have_result(op_results, op_cb)

        entity, err = op_results[self]
        if not err:
            op_results.next_ops += self.view.remove(entity)


class View(object):
    # The table used as input for this view. The view will be informed by
    # updates to this table.
    table = None

    # What table are we writing to.
    view_table = None

    def __init__(self, db):
        self.db = db

    def operations_for_operation(self, op):
        if isinstance(op, operation.PutOperation):
            return [], self.add(op.entity)
        elif isinstance(op, operation.DeleteOperation):
            return [GetAndRemoveOperation(op.table, op.key, self)], []
        elif isinstance(op, operation.UpdateOperation):
            log.warning("View %s doesn't know how to handle an update", self)
        elif isinstance(op, (operation.GetOperation, operation.QueryOperation)):
            pass
        else:
            raise NotImplementedError(op)

        return [], []

    def add(self, entity):
        return []

    def remove(self, entity):
        return []


