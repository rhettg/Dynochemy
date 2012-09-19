# -*- coding: utf-8 -*-
"""
This module contains objects for handling indexing in Dynochemy.

This is a framework where an index can be defined that either 'adds' or 'removes' a entity and 
then emits sets of operations that should be performed.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""



class Index(object):
    def add(self, entity):
        pass
    def remove(self, entity):
        pass

