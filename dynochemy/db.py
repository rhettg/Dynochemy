# -*- coding: utf-8 -*-
"""
This module contains the primary objects that powers Dynochemy.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import copy
import json
import time
import logging
import pprint
import collections

from tornado.ioloop import IOLoop
from asyncdynamo import asyncdynamo

from .errors import Error, SyncUnallowedError, DuplicateBatchItemError
from . import utils
from .defer import ResultErrorTupleDefer
from .defer import ResultErrorKWDefer


log = logging.getLogger(__name__)


class BaseDB(object):
    def __init__(self):
        self.allow_sync = True
        self.ioloop = None
        self.tables = {}
        self._tables_by_db_name = {}

    def register(self, table, create=False):
        self.tables[table.__name__] = table
        self._tables_by_db_name[table.name] = table

    def __getattr__(self, name):
        try:
            return self.tables[name](self)
        except KeyError:
            raise AttributeError

    def table_by_name(self, name):
        return self._tables_by_db_name[name](self)

    def batch_write(self):
        return WriteBatch(self)

    def batch_read(self):
        return ReadBatch(self)


class DB(BaseDB):
    def __init__(self, access_key, access_secret, ioloop=None):
        super(DB, self).__init__()

        self.allow_sync = bool(ioloop is None)
        self.ioloop = ioloop or IOLoop()
        self.client = asyncdynamo.AsyncDynamoDB(access_key, access_secret, ioloop=self.ioloop)


class Table(object):
    name = None
    hash_key = None
    range_key = None

    def __init__(self, db):
        self.db = db

    @property
    def key_spec(self):
        if self.range_key:
            return (self.hash_key, self.range_key)
        else:
            return (self.hash_key,)

    def _get(self, key, attributes=None, consistent=True, callback=None):
        data = {
                'TableName': self.name,
               }

        if self.has_range:
            data['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            data['Key'] = utils.format_key(('HashKeyElement',), (key,))

        if attributes:
            data['AttributesToGet'] = attributes

        data['ConsistentRead'] = consistent

        defer = None
        if callback is None:
            defer = ResultErrorTupleDefer(self.db.ioloop)
            callback = defer.callback

        def handle_result(data, error=None):
            if error is not None:
                return callback(None, error)

            if 'Item' in data:
                callback(utils.parse_item(data['Item']), None)
            else:
                callback(None, None)

        self.db.client.make_request('GetItem', body=json.dumps(data), callback=handle_result)
        return defer

    def get_async(self, key, callback, attributes=None, consistent=True):
        self._get(key, attributes=attributes, consistent=consistent, callback=callback)

    def get_defer(self, key, attributes=None, consistent=True):
        return self._get(key, attributes=attributes, consistent=consistent)

    def __getitem__(self, key):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._get(key)

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
            defer = ResultErrorTupleDefer(self.db.ioloop)
            callback = defer.callback

        def handle_result(data, error=None):
            if error is not None:
                callback(None, error)

            callback(None, None)

        self.db.client.make_request('PutItem', body=json.dumps(data), callback=handle_result)
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
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._put(value)

        result, error = d(timeout=timeout)
        if error:
            raise Error(error)

    def _delete(self, key, callback=None):
        data = {
                'TableName': self.name,
               }

        if self.has_range:
            data['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            data['Key'] = utils.format_key(('HashKeyElement',), (key,))

        data['ReturnValues'] = "ALL_OLD"

        defer = None
        if callback is None:
            defer = ResultErrorTupleDefer(self.db.ioloop)
            callback = defer.callback

        def handle_result(data, error=None):
            if error is not None:
                return callback(None, error)

            if 'Attributes' in data:
                callback(utils.parse_item(data['Attributes']), None)
            else:
                callback(None, None)

        self.db.client.make_request('DeleteItem', body=json.dumps(data), callback=handle_result)
        return defer

    def delete_async(self, key, callback):
        self._delete(key, callback=callback)

    def delete_defer(self, key):
        return self._delete(key)

    def __delitem__(self, key):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._delete(key)

        item, error =  d()
        if error:
            raise Error(error)

        if item is None:
            raise KeyError(key)

        return item

    delete = __delitem__

    def _update(self, key, add=None, put=None, delete=None, callback=None):
        data = {
                'TableName': self.name,
                'ReturnValues': 'ALL_NEW',
               }

        if self.has_range:
            data['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            data['Key'] = utils.format_key(('HashKeyElement',), [key])

        data['AttributeUpdates'] = {}

        if add:
            for attribute, value in add.iteritems():
                update = {attribute: {'Value': utils.format_value(value), 'Action': 'ADD'}}
                data['AttributeUpdates'].update(update)
        if put:
            for attribute, value in put.iteritems():
                update = {attribute: {'Value': utils.format_value(value), 'Action': 'PUT'}}
                data['AttributeUpdates'].update(update)

        if delete:
            for attribute, value in delete.iteritems():
                update = {attribute: {'Action': 'DELETE'}}
                if value is not None:
                    update[attribute]['Value'] = utils.format_value(value)
                data['AttributeUpdates'].update(update)

        defer = None
        if callback is None:
            defer = ResultErrorTupleDefer(ioloop=self.db.ioloop)
            callback = defer.callback

        def handle_result(result, error=None):
            if error is not None:
                callback(None, error)

            callback(result, None)

        self.db.client.make_request('UpdateItem', body=json.dumps(data), callback=handle_result)
        return defer

    def update(self, key, add=None, put=None, delete=None, timeout=None):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._update(key, add=add, put=put, delete=delete)

        result, error = d(timeout=timeout)
        if error:
            raise Error(error)

        return result

    def update_defer(self, key, add=None, put=None, delete=None, timeout=None):
        return self._update(key, add=add, put=put, delete=delete)

    def update_async(self, key, callback, add=None, put=None, delete=None):
        return self._update(key, add=add, put=put, delete=delete, callback=callback)

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
            return None

        query = copy.copy(result.query)
        query.args['ExclusiveStartKey'] = result.result_data['LastEvaluatedKey']
        return query

    def has_range(self):
        return bool(len(self.key_spec) == 2)

    def _item_key(self, item):
        """Return a tuple of identifying information for the item.

        This is based on the keys."""
        key = [item[k] for k in self.key_spec]
        return tuple(key)

    def _key_key(self, key):
        """Return a tuple of identifying information for the parsed key"""

        return (key['HashKeyElement'], key['RangeKeyElement'])




class Batch(object):
    """Object for doing a batch operation.

    The caller can do as many put, get or delete operations on the batch as needed, and the requests
    will be combined into one or more requests to DynamoDB

    Usage should be something like:

        b = db.batch_write()
        b.put(item1)
        b.put(item2)

        b()

        if b.errors:
            raise Exception

    Each operation returns a 'defer' object that can later retrieve results.
    Note that a batch is in no way atomic. Even the underlying batch operations provided by DynamoDB are not
    really atomic.

    Typically, a request will either error or not. However, some elements may be unprocessed due to exceeding capacity.
    In this case, the defer object for the operation will not be complete (`not d.done`).
    """

    def __init__(self, db):
        self.db = db

        # We use several data structures to manage our requests
        # Requests are identified by a key, the format varying based on type of request. Generally it's a tuple
        # and some uniquly identifying information.

        # This is our queue of requests that need processing
        self._requests = []

        # Map of request key to the actual request data
        self._request_data = {}

        # Map of request key to the defer we returned for it.
        self._request_defer = {}


        # Defer objects for outstanding batches. When this is empty, we're done.
        self._batch_defers = []

        # This defer tracks the over all status of this batch.
        # When all all 
        self._defer = ResultErrorKWDefer(ioloop=self.db.ioloop)

        self.errors = []

    def __getattr__(self, name):
        if hasattr(self.db, name):
            tbl = getattr(self.db, name)
            return BatchTable(self, tbl)
        else:
            raise AttributeError

    def __call__(self, timeout=None):
        d = self._run()
        return d()

    def async(self, callback=None):
        if callback:
            def defer_callback(df):
                callback(df.result)
            self._defer.add_callback(defer_callback)

        self._run()

    def defer(self):
        return self._run()

    def _batch_callback(self, deferred):
        log.debug("Callback for df %r", deferred)

        self._batch_defers.remove(deferred)

        # We may need to make another batch request
        self._run()

        if deferred.kwargs.get('error'):
            error = deferred.kwargs['error']
            error_data = json.loads(error.data)
            # TODO: We should handle this internally with some sort of exponential backoff
            if 'ProvisionedThroughputExceededException' in error_data['__type']:
                raise Exception('provision exceeded')

            self._defer.callback(None, error=deferred.kwargs['error'])
        else:
            if len(self._batch_defers) == 0 and not self._defer.done:
                # And we're done. We don't have any data to provide though.
                self._defer.callback(None)

    def _make_request(requests):
        raise NotImplementedError

    def _run(self):
        log.debug("Creating batches, %d requests outstanding", len(self._requests))

        while self._requests:
            request_group = []

            while len(request_group) < self.MAX_ITEMS and len(self._requests) > 0:
                request_group.append(self._requests.pop())

            self._run_batch(request_group)

        return self._defer



class BatchTable(object):
    def __init__(self, batch, table):
        self.table = table
        self.batch = batch

    def put(self, value):
        return self.batch.put(self.table, value)

    def delete(self, key):
        return self.batch.delete(self.table, key)

    def get(self, key):
        return self.batch.get(self.table, key)


class WriteBatch(Batch):
    MAX_ITEMS = 25

    def put(self, table, value):
        df = ResultErrorKWDefer(ioloop=self._defer.ioloop)
        args = {'PutRequest': {"Item": utils.format_item(value)}}

        req_key = (table.name, "PutRequest", table._item_key(value))
        if req_key in self._request_defer:
            raise DuplicateBatchItemError(value)

        log.debug("Building request %r", req_key)

        self._request_defer[req_key] = df
        self._request_data[req_key] = (table.name, args)
        self._requests.append(req_key)

        return df

    def delete(self, table, key):
        df = ResultErrorKWDefer(ioloop=self._defer.ioloop)
        req_args = {}
        if table.has_range:
            req_args['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            req_args['Key'] = utils.format_key(('HashKeyElement',), (key,))

        args = {'DeleteRequest': req_args}

        req_key = (table.name, "DeleteRequest", key)

        log.debug("Building request %r", req_key)

        self._request_defer[req_key] = df
        self._request_data[req_key] = (table.name, args)
        self._requests.append(req_key)

        return df

    def _run_batch(self, requests):
        log.debug("Creating BatchWrite request for %d items", len(requests))

        batch_defer = ResultErrorKWDefer(ioloop=self._defer.ioloop)
        batch_defer.add_callback(self._batch_callback)
        self._batch_defers.append(batch_defer)

        args = {"RequestItems": {}}

        for key in requests:
            table_name, req = self._request_data[key]
            args['RequestItems'].setdefault(table_name, []).append(req)

        def handle_result(data, error=None):
            if error is not None:
                log.error("Received error for batch: %r", error)

                for key in requests:
                    self._request_defer[key].callback(None, error=error)

                self.errors.append(error)
                batch_defer.callback(None, error=error)
            else:
                log.debug("Received successful result from batch: %r", data)

                unprocessed_keys = set()
                if data.get('UnprocessedItems'):
                    for table_name, unprocessed_items in data['UnprocessedItems'].iteritems():
                        table = self.db.table_for_name(table_name)
                        for item in unprocessed_items:
                            (req_type, req), = item.items()
                            key = table._key_key(item['Key'])
                            request = (table_name, req_type, key)

                            if request not in self._request_data:
                                log.warning("%r not found in %r", request, self._request_data.keys())
                                continue

                            assert request in self._request_data
                            unprocessed_keys.add(request)
                            self._requests.append(request)

                if unprocessed_keys:
                    log.warning("Found %d keys unprocessed", len(unprocessed_keys))

                for key in requests:
                    if key not in unprocessed_keys:
                        self._request_defer[key].callback(data)

                batch_defer.callback(data)

        self.db.client.make_request('BatchWriteItem', body=json.dumps(args), callback=handle_result)
        return batch_defer


class ReadBatch(Batch):
    MAX_ITEMS = 100

    def get(self, table, key):
        df = ResultErrorKWDefer(ioloop=self._defer.ioloop)
        if table.has_range:
            req_key = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            req_key = utils.format_key(('HashKeyElement',), (key,))

        log.debug("Building request %r", req_key)

        request = (table.name, key)

        self._request_defer[request] = df
        self._request_data[request] = (table.name, req_key)
        self._requests.append(request)

        return df

    def _run_batch(self, requests):
        log.debug("Creating ReadBatch request for %d items", len(requests))

        batch_defer = ResultErrorKWDefer(ioloop=self._defer.ioloop)
        batch_defer.add_callback(self._batch_callback)
        self._batch_defers.append(batch_defer)

        args = {"RequestItems": {}}

        for request in requests:
            table_name, key = self._request_data[request]
            args['RequestItems'].setdefault(table_name, {'Keys': []})['Keys'].append(key)

        def handle_result(data, error=None):
            if error is not None:
                log.error("Received error for batch: %r", error)

                for request in requests:
                    self._request_defer[request].callback(None, error=error)

                self.errors.append(error)
                batch_defer.callback(None, error=error)
            else:
                log.debug("Received successful result from batch: %r", data)

                unprocessed_keys = set()
                if data.get('UnprocessedItems'):
                    for table_name, unprocessed_items in data['UnprocessedItems'].iteritems():
                        table = self.db.table_by_name(table_name)

                        for req_key in unprocessed_items['Keys']:
                            key = table._key_key(req_key)
                            request = (table_name, key)

                            if request not in self._request_data:
                                log.warning("%r not found in %r", request, self._request_data.keys())
                                continue

                            assert request in self._request_data
                            unprocessed_keys.add(request)
                            self._requests.append(request)

                if unprocessed_keys:
                    log.warning("Found %d keys unprocessed", len(unprocessed_keys))

                # Extract all the items out of the response. This is slightly complicated because we need
                # to reconstitute what there request identifier must be (table name, (..key..))
                found_entities = {}
                for table_name, result in data.iteritems():
                    for item in result['Items']:
                        table = self.db.table_by_name(table_name)
                        entity = utils.parse_item(item)
                        key = table._item_key(entity)
                        request = (table_name, key)
                        found_entities[request] = entity

                for request in requests:
                    if request not in unprocessed_keys:
                        self._request_defer[request].callback(found_entities[request])

                batch_defer.callback(data)

        self.db.client.make_request('BatchGetItem', body=json.dumps(args), callback=handle_result)
        return batch_defer


class Scan(object):
    def __init__(self, table):
        self.table = table
        self.args = {
                     'TableName': self.table.name, 
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
            defer = ResultErrorTupleDefer(self.table.db.ioloop)
            callback = defer.callback

        def handle_result(data, error=None):
            if error is not None:
                callback(None, error)

            callback(ScanResults(self, data), None)

        self.table.db.client.make_request('Scan', body=json.dumps(self.args), callback=handle_result)
        return defer

    def __call__(self, timeout=None):
        if not self.table.db.allow_sync:
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
    def __init__(self, table, hash_key):
        self.table = table 
        self.args = {
                     'TableName': self.table.name, 
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
        if not self.table.range_key:
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
            defer = ResultErrorTupleDefer(self.table.db.ioloop)
            callback = defer.callback

        def handle_result(result_data, error=None):
            results = None
            if error is None:
                results = QueryResults(self, result_data)

            return callback(results, error)

        self.table.db.client.make_request('Query', body=json.dumps(self.args), callback=handle_result)
        return defer

    def __call__(self, timeout=None):
        if not self.table.db.allow_sync:
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

