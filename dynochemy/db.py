# -*- coding: utf-8 -*-
"""
This module contains the primary objects that powers Dynochemy.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import copy
import json
import time

from tornado.ioloop import IOLoop
from asyncdynamo import asyncdynamo

from .errors import Error
from . import utils
from .defer import Defer

class SyncUnallowedError(Error): pass


class DB(object):
    def __init__(self, name, key_spec, access_key, access_secret, ioloop=None):
        self.name = name
        self.key_spec = key_spec
        self.allow_sync = bool(ioloop is None)
        self.ioloop = ioloop or IOLoop()

        self._asyncdynamo = asyncdynamo.AsyncDynamoDB(access_key, access_secret, ioloop=self.ioloop)

    def _get(self, key, attributes=None, consistent=True, callback=None, timeout=None):
        data = {
                'TableName': self.name,
               }

        if self.has_range:
            data['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            data['Key'] = utils.format_key(('HashKeyElement',), [key])

        if attributes:
            data['AttributesToGet'] = attributes

        data['ConsistentRead'] = consistent

        defer = None
        if callback is None:
            defer = Defer(self.ioloop, timeout=timeout)
            callback = defer.callback

        self._asyncdynamo.make_request('GetItem', body=json.dumps(data), callback=callback)
        return defer

    def get_async(self, key, callback, attributes=None, consistent=True):
        self._get(key, attributes=attributes, consistent=consistent, callback=callback)

    def get_defer(self, key, attributes=None, consistent=True):
        return self._get(key, attributes=attributes, consistent=consistent)

    def __getitem__(self, key):
        if not self.allow_sync:
            raise SyncUnallowedError()

        d = self._get(key)

        args, kwargs =  d()
        if kwargs.get('error'):
            raise Error(kwargs['error'])
        elif 'Item' in args[0]:
            return utils.parse_item(args[0]['Item'])
        else:
            return None

    get = __getitem__

    def _put(self, value, callback=None, timeout=None):
        data = {
                'TableName': self.name,
               }

        item = utils.format_item(value)
        data['Item'] = item

        defer = None
        if callback is None:
            defer = Defer(self.ioloop, timeout=timeout)
            callback = defer.callback

        self._asyncdynamo.make_request('PutItem', body=json.dumps(data), callback=callback)
        return defer

    def put_async(self, value, callback):
        self._put(value, callback=callback)

    def put_defer(self, value):
        return self._put(value)

    def __setitem__(self, key, value):
        item = copy.copy(value)
        for key, key_value in zip(self.key_spec, key):
            item[key] = key_value
        self.put(item)

    def put(self, value):
        if not self.allow_sync:
            raise SyncUnallowedError()

        d = self._put(value)
        args, kwargs = d()
        if kwargs.get('error'):
            raise Error(kwargs['error'])

    def __delitem__(self, key):
        pass

    def _scan(self, callback=None, timeout=None):
        data = {
                'TableName': self.name,
               }

        defer = None
        if callback is None:
            defer = Defer(self.ioloop, timeout=timeout)
            callback = defer.callback

        self._asyncdynamo.make_request('Scan', body=json.dumps(data), callback=callback)
        return defer

    def scan(self):
        if not self.allow_sync:
            raise SyncUnallowedError()

        d = self._scan()
        args, kwargs = d()
        result = args[0]
        return [utils.parse_item(i) for i in result['Items']]

    def query(self, hash_key):
        return Query(self, hash_key)

    def query_next(self, result):
        if not result.has_next:
            raise ValueError("Result has no more")

        query = copy.copy(result.query)
        query.args['ExclusiveStartKey'] = result.args['LastEvaluatedKey']
        return query

    def has_range(self):
        return bool(len(self.key_spec) == 2)


class Query(object):
    def __init__(self, db, hash_key):
        self.db = db
        self.args = {
                     'TableName': self.db.name, 
                     'HashKeyValue': utils.format_value(hash_key),
                     'ConsistentRead': True,
                    }

    def attributes(self, *args):
        query = copy.copy(self)
        query.args['AttributesToGet'] = args
        return query
        
    def range(self, start, end=None):
        if not self.db.has_range:
            raise ValueError("Must have range")

        query = copy.copy(self)
        query.args['RangeKeyCondition'] = [{self.db.key[1]: utils.format_value(start)}]
        if end is not None:
            query.args['RangeKeyCondition'].append({self.db.key[1]: utils.format_value(end)})

    def reverse(self, reverse=True):
        query = copy.copy(self)
        query.args['ScanIndexForward'] = not reverse
        return query
        
    def limit(self, limit):
        query = copy.copy(self)
        query.args['Limit'] = limit
        return query


class Results(object):
    def __init__(self, args):
        self.args = args

    def __iter__(self):
        return (utils.parse_item(item) for item in self.args['Items'])

    def __len__(self):
        return self.args['Count']

    @property
    def has_next(self):
        return bool('LastEvaluatedKey' in self.args)
