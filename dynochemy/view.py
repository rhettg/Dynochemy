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


class View(object):
    # The table used as input for this view. The view will be informed by
    # updates to this table.
    table = None

    # What table are we writing to.
    view_table = None

    def __init__(self, db):
        # We allow instances of this to exist to provide some amount of parallelism with tables.
        # But i'm not yet quite sure if we really need to. All the important methods are classmethods
        self.db = db

    @classmethod
    def operations_for_operation(cls, op, result):
        if isinstance(op, operation.PutOperation):
            return cls.add(op, result)
        elif isinstance(op, operation.GetAndDeleteOperation):
            return cls.remove(op, result)
        elif isinstance(op, operation.UpdateOperation):
            return cls.update(op, result)
        elif isinstance(op, operation.DeleteOperation):
            # In a solvent, first we do a GetAndDeleteOperation, so this should have already been handled.
            pass
        elif isinstance(op, (operation.GetOperation, operation.QueryOperation)):
            pass
        else:
            raise NotImplementedError(op)

        return []

    @classmethod
    def add(cls, op, result):
        return []

    @classmethod
    def remove(cls, op, result):
        return []

    @classmethod
    def update(cls, op, result):
        return []


