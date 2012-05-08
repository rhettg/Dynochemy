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
from .defer import ResultErrorDefer

class SyncUnallowedError(Error): pass


class BaseDB(object):
    def __init__(self, name, key_spec):
        self.name = name
        self.key_spec = key_spec
        self.allow_sync = True
        self.ioloop = None

    def _get(self, key, attributes=None, consistent=True, callback=None):
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
            defer = ResultErrorDefer(self.ioloop)
            callback = defer.callback

        def handle_result(data, error=None):
            if error is not None:
                callback(None, error)

            if 'Item' in data:
                callback(utils.parse_item(data['Item']), None)
            else:
                callback(None, None)

        self.client.make_request('GetItem', body=json.dumps(data), callback=handle_result)
        return defer

    def get_async(self, key, callback, attributes=None, consistent=True):
        self._get(key, attributes=attributes, consistent=consistent, callback=callback)

    def get_defer(self, key, attributes=None, consistent=True):
        return self._get(key, attributes=attributes, consistent=consistent)

    def __getitem__(self, key):
        if not self.allow_sync:
            raise SyncUnallowedError()

        if len(self.key_spec) > 1:
            d = self._get(key)
        else:
            d = self._get((key,))

        item, error =  d()
        if error:
            raise Error(error)

        if item is None:
            raise KeyError(key)

        return item

    get = __getitem__

    def _put(self, value, callback=None):
        data = {
                'TableName': self.name,
               }

        item = utils.format_item(value)
        data['Item'] = item

        defer = None
        if callback is None:
            defer = ResultErrorDefer(self.ioloop)
            callback = defer.callback

        def handle_result(data, error=None):
            if error is not None:
                callback(None, error)

            callback(None, None)

        self.client.make_request('PutItem', body=json.dumps(data), callback=handle_result)
        return defer

    def put_async(self, value, callback):
        self._put(value, callback=callback)

    def put_defer(self, value):
        return self._put(value)

    def __setitem__(self, key, value):
        item = copy.copy(value)
        if len(self.key_spec) > 1:
            for key, key_value in zip(self.key_spec, key):
                item[key] = key_value
        else:
            item[self.key_spec[0]] = key

        self.put(item)

    def put(self, value, timeout=None):
        if not self.allow_sync:
            raise SyncUnallowedError()

        d = self._put(value)

        result, error = d(timeout=timeout)
        if error:
            raise Error(error)

    def __delitem__(self, key):
        pass

    def scan(self):
        return Scan(self)

    def scan_next(self, result):
        if not result.has_next:
            raise ValueError("Result has no more")

        scan = copy.copy(result.scan)
        scan.args['ExclusiveStartKey'] = result.result_data['LastEvaluatedKey']
        return scan

    def query(self, hash_key):
        return Query(self, hash_key)

    def query_next(self, result):
        if not result.has_next:
            raise ValueError("Result has no more")

        query = copy.copy(result.query)
        query.args['ExclusiveStartKey'] = result.result_data['LastEvaluatedKey']
        return query

    def has_range(self):
        return bool(len(self.key_spec) == 2)


class DB(BaseDB):
    def __init__(self, name, key_spec, access_key, access_secret, ioloop=None):
        super(DB, self).__init__(name, key_spec)

        self.allow_sync = bool(ioloop is None)
        self.ioloop = ioloop or IOLoop()
        self.client = asyncdynamo.AsyncDynamoDB(access_key, access_secret, ioloop=self.ioloop)


class Scan(object):
    def __init__(self, db):
        self.db = db
        self.args = {
                     'TableName': self.db.name, 
                    }

    def limit(self, limit):
        scan = copy.copy(self)
        scan.args['Limit'] = limit
        return scan


    def _filter(self, name, value, compare):
        scan = copy.copy(self)
        scan.args.setdefault('ScanFilter', {})
        scan.args['ScanFilter'].setdefault(name, {})
        scan.args['ScanFilter'][name]['ComparisonOperator'] = compare
        scan.args['ScanFilter'][name].setdefault('AttributeValueList', [])
        scan.args['ScanFilter'][name]['AttributeValueList'].append(utils.format_value(value))
        return scan

    def filter_eq(self, name, value):
        return self._filter(name, value, "EQ")

    def filter_lt(self, name, value):
        return self._filter(name, value, "LT")

    def filter_gt(self, name, value):
        return self._filter(name, value, "GT")

    def _scan(self, callback=None):
        defer = None
        if callback is None:
            defer = ResultErrorDefer(self.db.ioloop)
            callback = defer.callback

        def handle_result(data, error=None):
            if error is not None:
                callback(None, error)

            callback(ScanResults(self, data), None)

        self.db.client.make_request('Scan', body=json.dumps(self.args), callback=handle_result)
        return defer

    def __call__(self, timeout=None):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._scan()
        data, error = d(timeout=timeout)
        if error:
            raise Error(error)

        return data

    def defer(self):
        return self._scan()

    def async(self, callback=None):
        self._scan(callback=callback)


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
        query.args['AttributesToGet'] = args[0]
        return query
        
    def range(self, start=None, end=None):
        """Includes a condition where by range keys start at 'start' (inclusive) and are less than 'end' (exclusive)
        
        """
        if not self.db.has_range:
            raise ValueError("Must have range")

        query = copy.copy(self)
        query.args['RangeKeyCondition'] = condition = {'AttributeValueList': [], 'ComparisonOperator': None}
        if None not in (start, end):
            # We have a complete start to end condition
            condition['AttributeValueList'].append(utils.format_value(start))
            condition['AttributeValueList'].append(utils.format_value(end))
            condition['ComparisonOperator'] = "BETWEEN"

        elif start is not None:
            condition['AttributeValueList'].append(utils.format_value(start))
            condition['ComparisonOperator'] = "GE"

        elif end is not None:
            condition['AttributeValueList'].append(utils.format_value(end))
            condition['ComparisonOperator'] = "LT"

        return query

    def reverse(self, reverse=True):
        query = copy.copy(self)
        query.args['ScanIndexForward'] = not reverse
        return query
        
    def limit(self, limit):
        query = copy.copy(self)
        query.args['Limit'] = limit
        return query

    def _query(self, callback=None):
        defer = None
        if callback is None:
            defer = ResultErrorDefer(self.db.ioloop)
            callback = defer.callback

        def handle_result(result_data, error=None):
            results = None
            if error is None:
                results = QueryResults(self, result_data)

            return callback(results, error)

        self.db.client.make_request('Query', body=json.dumps(self.args), callback=handle_result)
        return defer

    def __call__(self, timeout=None):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._query()
        results, error = d(timeout=timeout)
        if error:
            raise Error(error)

        return results

    def defer(self):
        return self._query()

    def async(self, callback=None):
        self._query(callback=callback)



class Results(object):
    def __init__(self, result_data):
        self.result_data = result_data

    def __iter__(self):
        return (utils.parse_item(item) for item in self.result_data['Items'])

    def __len__(self):
        return self.result_data['Count']

    def __getitem__(self, key):
        return utils.parse_item(self.result_data['Items'][key])

    @property
    def has_next(self):
        return bool('LastEvaluatedKey' in self.result_data)


class QueryResults(Results):
    def __init__(self, query, result_data):
        self.query = query
        self.result_data = result_data


class ScanResults(Results):
    def __init__(self, scan, result_data):
        self.scan = scan
        self.result_data = result_data

