# -*- coding: utf-8 -*-
"""
This module contains the primary objects that power Bootstrap.

:copyright: (c) 2012 by Firstname Lastname.
:license: ISC, see LICENSE for more details.
"""
import copy
import json
import functools
import time

from tornado.ioloop import IOLoop
from asyncdynamo import asyncdynamo

from .errors import Error
from . import utils


class Defer(object):
    def __init__(self, ioloop=None, timeout=None):
        self.done = False
        self.ioloop = ioloop
        self.do_stop = False
        self.timeout = timeout
        self._timeout_req = None

    def callback(self, *args, **kwargs):
        self.done = True
        self.args = args
        self.kwargs = kwargs

        if self._timeout_req:
            self.ioloop.remove_timeout(self._timeout_req)
            self._timeout_req = None

        if self.do_stop:
            self.ioloop.stop()

    def __call__(self):
        if not self.ioloop:
            raise ValueError("IOLoop required")

        if not self.done:
            self.do_stop = True
            if self.timeout:
                self._timeout_req = self.ioloop.add_timeout(time.time() + self.timeout, functools.partial(self.callback, error='Timeout'))

            self.ioloop.start()

        assert self.done
        return (self.args, self.kwargs)


class DB(object):
    def __init__(self, name, key_spec, access_key, access_secret, ioloop=None):
        self.name = name
        self.key_spec = key_spec
        self.allow_async = bool(ioloop is not None)
        self.ioloop = ioloop or IOLoop()

        self._asyncdynamo = asyncdynamo.AsyncDynamoDB(access_key, access_secret, ioloop=self.ioloop)

    def _get(self, key, attributes=None, consistent=True, callback=None, timeout=None):
        data = {
                'TableName': self.name,
                'Key': utils.format_key(self.key_spec, key)
               }

        if attributes:
            data['AttributesToGet'] = attributes

        data['ConsistentRead'] = consistent

        defer = None
        if callback is None:
            defer = Defer(self.ioloop, timeout=timeout)
            callback = defer.callback

        self._asyncdynamo.make_request('GetItem', body=json.dumps(data), callback=callback)
        return defer

    def __getitem__(self, key):
        d = self._get(key, timeout=5)

        args, kwargs =  d()
        if kwargs.get('error'):
            raise Error(kwargs['error'])
        elif 'Item' in args[0]:
            return utils.parse_item(args[0]['Item'])
        else:
            raise ValueError(args)

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

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
