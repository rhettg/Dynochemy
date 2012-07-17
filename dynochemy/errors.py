# -*- coding: utf-8 -*-

"""
This module contains the set of Dynochemy's exceptions

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.

"""


class Error(Exception):
    """This is an ambiguous error that occured."""
    pass

class SyncUnallowedError(Error): pass

class DuplicateBatchItemError(Error): pass


__all__ = ["Error", "SyncUnallowedError", "DuplicateBatchItemError"]
