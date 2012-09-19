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
from . import operation

def view_operations(db, op):
    """Given the 'db' as context, find all our views and generate additional
    operations that should result from the user supplied operation
    """
    out = []
    for view in db.views_by_table(op.table):
        out += view.operations_for_operation(op)

    return out


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
            return self.add(op.entity)
        elif isinstance(op, operation.DeleteOperation):
            return self.remote(op.entity)
        elif isinstance(op, operation.UpdateOperation):
            log.warning("View %s doesn't know how to handle an update", self)
        else:
            raise NotImplementedError(op)

    def add(self, entity):
        return []

    def remove(self, entity):
        return []


