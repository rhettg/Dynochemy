# -*- coding: utf-8 -*-
"""
This module contains the primary objects that power Bootstrap.

:copyright: (c) 2012 by Firstname Lastname.
:license: ISC, see LICENSE for more details.
"""

from .errors import Error

def build_key(key_spec, key_value):
    out_key = {}
    for key, value in zip(key_spec, key_value):
        if isinstance(value, basestring):
            out_key[key] = {'S': value}
        elif isinstance(value, (int, float)):
            out_key[key] = {'N': str(value)}
        else:
            raise ValueError(value)


class DB(object):
    def __init__(self, name, key, access_key, access_secret):
        self.name = name
        self.key = key

        self._asyncdynamo = asyncdynamo.AsyncDynamoDB(access_key, access_secret)

    def __getitem__(self, key):
        item_key = build_key(self.key, key)
        self._asyncdynamo.get_item(self.name, item_key, callback)

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def query(self, hash_key):
        return Query(self, hash_key)

    def query_next(self, hash_key):
        return Query(self, hash_key)


class Query(object):
    def __init__(self, db, hash_key):
        self.db = db
        self.hash_key = hash_key

    def range(self, start, end):
        pass

    def reverse(self, reverse=True):
        pass

    def limit(self, limit):
        pass


class Results(object):
    def __init__(self):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass

    @property
    def has_next(self):
        return False
