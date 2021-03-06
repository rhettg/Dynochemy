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

from asyncdynamo import asyncdynamo

from .errors import Error, SyncUnallowedError, DuplicateBatchItemError, UnprocessedItemError, ExceededBatchRequestsError, ItemNotFoundError, parse_error
from . import utils
from .defer import Defer
from . import view
from . import constants
from . import utils


log = logging.getLogger(__name__)


class BaseDB(object):
    def __init__(self):
        self.allow_sync = True
        self.ioloop = None
        self.tables = {}
        self.views = {}
        self._tables_by_db_name = {}
        self._views_by_table = collections.defaultdict(list)

    def register(self, cls, create=False):
        if issubclass(cls, Table):
            self.tables[cls.__name__] = instance = cls(self)
            self._tables_by_db_name[cls.name] = instance
        elif issubclass(cls, view.View):
            self.views[cls.__name__] = instance = cls(self)
            self._views_by_table[cls.table.__name__].append(instance)

    def __getattr__(self, name):
        try:
            return self.tables[name]
        except KeyError:
            try:
                return self.views[name]
            except KeyError:
                raise AttributeError(name)

    def table_by_name(self, name):
        return self._tables_by_db_name[name]

    def views_by_table(self, table):
        return self._views_by_table[table.__name__]

    def batch_write(self):
        return WriteBatch(self)

    def batch_read(self):
        return ReadBatch(self)


class DB(BaseDB):
    def __init__(self, access_key, access_secret, ioloop=None):
        super(DB, self).__init__()

        self.allow_sync = bool(ioloop is None)
        self.ioloop = ioloop or utils.patch_io_loop()()

        # This will monkey patch the _HTTPConnection class in tornado
        # Eventually when the real patch is released, we should do some sort of version check.
        utils.patch_http_client()

        self.client = asyncdynamo.AsyncDynamoDB(access_key, access_secret, ioloop=self.ioloop)


class Table(object):
    name = None
    hash_key = None
    range_key = None

    read_capacity = None
    write_capacity = None

    def __init__(self, db):
        self.db = db
        self.read_counter = utils.ResourceCounter()
        self.write_counter = utils.ResourceCounter()

    @property
    def key_spec(self):
        if self.range_key:
            return (self.hash_key, self.range_key)
        else:
            return (self.hash_key,)

    def _record_read_capacity(self, value):
        log.debug("%.1f read capacity units consumed", value)
        self.read_counter.record(value)

    def _record_write_capacity(self, value):
        log.debug("%.1f write capacity units consumed", value)
        self.write_counter.record(value)

    def _get(self, key, attributes=None, consistent=True):
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

        defer = Defer(self.db.ioloop)

        def handle_result(data, error=None):
            if error is not None:
                defer.exception(ex=parse_error(error))
                return

            read_capacity = {self.name: 0.0}
            if 'ConsumedCapacityUnits' in data:
                read_capacity[self.name] = float(data['ConsumedCapacityUnits'])
                self._record_read_capacity(read_capacity[self.name])

            if 'Item' in data:
                defer.callback(utils.parse_item(data['Item']), read_capacity=read_capacity)
            else:
                defer.callback(None, read_capacity=read_capacity)

        self.db.client.make_request('GetItem', body=json.dumps(data), callback=handle_result)
        return defer

    def get_async(self, key, callback, attributes=None, consistent=True):
        df = self._get(key, attributes=attributes, consistent=consistent)

        def handle_result(get_df):
            callback(get_df.result)

        df.add_callback(handle_result)

    def get_defer(self, key, attributes=None, consistent=True):
        return self._get(key, attributes=attributes, consistent=consistent)

    def __getitem__(self, key):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._get(key)

        args, kwargs = d()

        if not args or args[0] is None:
            raise KeyError(key)

        return args[0]

    get = __getitem__

    def _put(self, value):
        data = {
                'TableName': self.name,
               }

        item = utils.format_item(value)
        data['Item'] = item

        defer = Defer(self.db.ioloop)

        def handle_result(data, error=None):
            if error is not None:
                defer.exception(ex=parse_error(error))
            else:
                write_capacity = {self.name: 0.0}
                if 'ConsumedCapacityUnits' in data:
                    write_capacity[self.name] = float(data['ConsumedCapacityUnits'])
                    self._record_write_capacity(write_capacity[self.name])

                defer.callback(None, write_capacity=write_capacity)

        self.db.client.make_request('PutItem', body=json.dumps(data), callback=handle_result)
        return defer

    def put_async(self, value, callback):
        df = self._put(value)

        def handle_result(put_df):
            callback(put_df.result)

        df.add_callback(handle_result)

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

        args, kwargs = d(timeout=timeout)
        return args and args[0]

    def _delete(self, key):
        data = {
                'TableName': self.name,
               }

        if self.has_range:
            data['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            data['Key'] = utils.format_key(('HashKeyElement',), (key,))

        data['ReturnValues'] = "ALL_OLD"

        defer = Defer(self.db.ioloop)

        def handle_result(data, error=None):
            if error is not None:
                defer.exception(ex=parse_error(error))
                return

            write_capacity = {self.name: 0.0}
            if 'ConsumedCapacityUnits' in data:
                write_capacity[self.name] = float(data['ConsumedCapacityUnits'])
                self._record_write_capacity(write_capacity[self.name])

            if 'Attributes' in data:
                defer.callback(utils.parse_item(data['Attributes']), write_capacity=write_capacity)
            else:
                defer.callback(None, write_capacity=write_capacity)

        self.db.client.make_request('DeleteItem', body=json.dumps(data), callback=handle_result)
        return defer

    def delete_async(self, key, callback):
        df = self._delete(key)

        def handle_result(del_df):
            callback(del_df.result)

        df.add_callback(handle_result)

    def delete_defer(self, key):
        return self._delete(key)

    def __delitem__(self, key):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._delete(key)

        args, kwargs =  d()

        if not args or args[0] is None:
            raise KeyError(key)

        return args[0]

    delete = __delitem__

    def _update(self, key, add=None, put=None, delete=None, return_value=None):
        data = {
                'TableName': self.name,
                'ReturnValues': return_value or 'ALL_NEW',
               }

        if self.has_range:
            data['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            data['Key'] = utils.format_key(('HashKeyElement',), [key])

        data['AttributeUpdates'] = {}

        if add:
            for attribute, value in add.iteritems():
                if value not in ('', None):
                    update = {attribute: {'Value': utils.format_value(value), 'Action': 'ADD'}}
                    data['AttributeUpdates'].update(update)
        if put:
            for attribute, value in put.iteritems():
                if attribute in self.key_spec:
                    raise ValueError("can't put key attributes")
                if value not in ('', None):
                    update = {attribute: {'Value': utils.format_value(value), 'Action': 'PUT'}}
                    data['AttributeUpdates'].update(update)

        if delete:
            for attribute, value in delete.iteritems():
                update = {attribute: {'Action': 'DELETE'}}
                if value is not None:
                    update[attribute]['Value'] = utils.format_value(value)
                data['AttributeUpdates'].update(update)

        defer = Defer(ioloop=self.db.ioloop)

        def handle_result(result, error=None):
            if error is not None:
                defer.exception(ex=parse_error(error))
                return

            write_capacity = 0.0
            if 'ConsumedCapacityUnits' in result:
                write_capacity = float(result['ConsumedCapacityUnits'])
                self._record_write_capacity(write_capacity)

            if 'Attributes' in result:
                defer.callback(utils.parse_item(result['Attributes']), write_capacity={self.name: write_capacity})
            else:
                defer.callback(None, write_capacity={self.name: write_capacity})

        self.db.client.make_request('UpdateItem', body=json.dumps(data), callback=handle_result)
        return defer

    def update(self, key, add=None, put=None, delete=None, return_value=None, timeout=None):
        if not self.db.allow_sync:
            raise SyncUnallowedError()

        d = self._update(key, add=add, put=put, delete=delete, return_value=return_value)

        args, kwargs = d(timeout=timeout)

        return args[0]

    def update_defer(self, key, add=None, put=None, delete=None, return_value=None, timeout=None):
        return self._update(key, add=add, put=put, delete=delete, return_value=return_value)

    def update_async(self, key, callback, add=None, put=None, delete=None):
        df = self._update(key, add=add, put=put, delete=delete)

        def handle_result(update_df):
            callback(update_df.result)

        df.add_callback(handle_result)

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

    @property
    def has_range(self):
        return bool(len(self.key_spec) == 2)

    def _item_key(self, item):
        """Return a tuple of identifying information for the item.

        This is based on the keys."""
        key = [utils.parse_value(item[k]) for k in self.key_spec]
        return tuple(key)

    def _key_key(self, value):
        """Return a tuple of identifying information for the parsed key"""

        key = [utils.parse_value(value[name]) for name, _ in zip(('HashKeyElement', 'RangeKeyElement'), self.key_spec)]
        return tuple(key)


class Batch(object):
    """Object for doing a batch operation.

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

        # Our requests that need to be processed
        self._requests = []

        # Map of request key to the actual request data
        # TODO: needed?
        self._request_data = {}

        # Map of request key to the defer we returned for it.
        self._request_defer = {}

        # This defer tracks the over all status of this batch.
        self._defer = Defer(ioloop=self.db.ioloop)

        self.errors = []

    def __getattr__(self, name):
        if hasattr(self.db, name):
            tbl = getattr(self.db, name)
            return BatchTable(self, tbl)
        else:
            raise AttributeError

    def __call__(self, timeout=None):
        self._run_batch()

        args, kwargs = self._defer(timeout=timeout)
        return args[0]

    def async(self, callback=None):
        if callback:
            def defer_callback(df):
                callback(df.result)
            self._defer.add_callback(defer_callback)

        self._run_batch()

    def defer(self):
        self._run_batch()
        return self._defer

    def _make_request(requests):
        raise NotImplementedError


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
    def put(self, table, value):
        df = Defer(ioloop=self._defer.ioloop)
        args = {'PutRequest': {"Item": utils.format_item(value)}}

        req_key = (table.name, "PutRequest", table._item_key(args['PutRequest']['Item']))
        if req_key in self._request_defer:
            raise DuplicateBatchItemError(value)

        log.debug("Building request %r", req_key)

        if len(self._requests) >= constants.MAX_BATCH_WRITE_ITEMS:
            raise ExceededBatchRequestsError("Too many requests")

        self._request_defer[req_key] = df
        self._request_data[req_key] = (table.name, args)
        self._requests.append(req_key)

        return df

    def delete(self, table, key):
        df = Defer(ioloop=self._defer.ioloop)
        req_args = {}
        if table.has_range:
            req_args['Key'] = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            req_args['Key'] = utils.format_key(('HashKeyElement',), (key,))

        args = {'DeleteRequest': req_args}

        req_key = (table.name, "DeleteRequest", key)

        log.debug("Building request %r", req_key)

        if len(self._requests) >= constants.MAX_BATCH_WRITE_ITEMS:
            raise ExceededBatchRequestsError("Too many requests")

        self._request_defer[req_key] = df
        self._request_data[req_key] = (table.name, args)
        self._requests.append(req_key)

        return df

    def _run_batch(self):
        log.debug("Creating BatchWrite request for %d items", len(self._requests))

        args = {"RequestItems": {}}

        for key in self._requests:
            table_name, req = self._request_data[key]
            args['RequestItems'].setdefault(table_name, []).append(req)

        def handle_result(data, error=None):
            if error is not None:
                real_error = parse_error(error)
                log.warning("Received error for batch: %r", real_error)

                for key in self._requests:
                    self._request_defer[key].exception(ex=real_error)

                self.errors.append(real_error)
                self._defer.exception(ex=real_error)
            else:
                log.debug("Received successful result from batch: %r", data)

                write_capacity = collections.defaultdict(float)
                if 'Responses' in data:
                    for table_name, response_data in data['Responses'].iteritems():
                        write_capacity[table_name] += float(response_data['ConsumedCapacityUnits'])
                        self.db.table_by_name(table_name)._record_write_capacity(float(response_data['ConsumedCapacityUnits']))

                unprocessed_items = set()
                if data.get('UnprocessedItems'):
                    for table_name, items in data['UnprocessedItems'].iteritems():
                        table = self.db.table_by_name(table_name)
                        for item in items:
                            (req_type, req), = item.items()

                            # Responses may be items or keys depending on the type of request
                            if 'Item' in req:
                                key = table._item_key(req['Item'])
                            else:
                                if table.has_range:
                                    key = table._key_key(req['Key'])
                                else:
                                    key = table._key_key(req['Key'])[0]

                            request = (table_name, req_type, key)

                            if request not in self._request_data:
                                log.warning("%r not found in %r", request, self._request_data.keys())
                                continue

                            self._request_defer[request].exception(ex=UnprocessedItemError())
                            unprocessed_items.add(request)

                if unprocessed_items:
                    log.warning("Found %d items unprocessed in BatchWrite", len(unprocessed_items))

                for key in self._requests:
                    if key not in unprocessed_items:
                        self._request_defer[key].callback(data)

                self._defer.callback(data, write_capacity=write_capacity)

        self.db.client.make_request('BatchWriteItem', body=json.dumps(args), callback=handle_result)


class ReadBatch(Batch):
    def get(self, table, key):
        df = Defer(ioloop=self._defer.ioloop)
        if table.has_range:
            req_key = utils.format_key(('HashKeyElement', 'RangeKeyElement'), key)
        else:
            req_key = utils.format_key(('HashKeyElement',), (key,))

        log.debug("Building request %r", req_key)

        if table.has_range:
            request = (table.name, key)
        else:
            request = (table.name, (key,))

        if len(self._requests) >= constants.MAX_BATCH_READ_ITEMS:
            raise ExceededBatchRequestsError("Too many requests")

        self._request_defer[request] = df
        self._request_data[request] = (table.name, req_key)
        self._requests.append(request)

        return df

    def _run_batch(self):
        log.debug("Creating ReadBatch request for %d items", len(self._requests))

        args = {"RequestItems": {}}

        for request in self._requests:
            table_name, key = self._request_data[request]
            args['RequestItems'].setdefault(table_name, {'Keys': []})['Keys'].append(key)

        def handle_result(data, error=None):
            if error is not None:
                real_error = parse_error(error)
                log.error("Received error for batch: %r", real_error)

                for request in self._requests:
                    self._request_defer[request].exception(ex=real_error)

                self._defer.exception(ex=real_error)
            else:
                log.debug("Received successful result from batch: %r", data)

                unprocessed_keys = set()
                if data.get('UnprocessedKeys'):
                    for table_name, unprocessed_items in data['UnprocessedKeys'].iteritems():
                        table = self.db.table_by_name(table_name)

                        for req_key in unprocessed_items['Keys']:
                            key = table._key_key(req_key)
                            request = (table_name, key)

                            if request not in self._request_data:
                                log.warning("%r not found in %r", request, self._request_data.keys())
                                continue

                            assert request in self._request_data
                            self._request_defer[request].exception(ex=UnprocessedItemError())
                            unprocessed_keys.add(request)

                if unprocessed_keys:
                    log.warning("Found %d keys unprocessed in BatchRead", len(unprocessed_keys))

                read_capacity = collections.defaultdict(float)
                for table_name, result in data['Responses'].iteritems():

                    if 'ConsumedCapacityUnits' in result:
                        read_capacity[table_name] += float(result['ConsumedCapacityUnits'])
                        self.db.table_by_name(table_name)._record_read_capacity(float(result['ConsumedCapacityUnits']))

                    for item in result['Items']:
                        table = self.db.table_by_name(table_name)
                        assert item
                        entity = utils.parse_item(item)
                        key = table._item_key(item)
                        request = (table_name, key)
                        self._request_defer[request].callback(entity)

                # Look for any items we didn't get results for.
                for req, df in self._request_defer.iteritems():
                    if not df.done:
                        self._request_defer[req].exception(ex=ItemNotFoundError())

                self._defer.callback(data, read_capacity=read_capacity)

        self.db.client.make_request('BatchGetItem', body=json.dumps(args), callback=handle_result)


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

    def _scan(self):
        defer = Defer(self.table.db.ioloop)

        def handle_result(data, error=None):
            if error is not None:
                defer.exception(ex=parse_error(error))
                return

            read_capacity = 0.0
            if 'ConsumedCapacityUnits' in data:
                read_capacity = float(data['ConsumedCapacityUnits'])
                self.table._record_read_capacity(read_capacity)

            defer.callback(ScanResults(self, data), read_capacity=read_capacity)

        self.table.db.client.make_request('Scan', body=json.dumps(self.args), callback=handle_result)
        return defer

    def __call__(self, timeout=None):
        if not self.table.db.allow_sync:
            raise SyncUnallowedError()

        d = self._scan()
        args, kwargs = d(timeout=timeout)
        return args[0]

    def defer(self):
        return self._scan()

    def async(self, callback=None):
        df = self._scan()

        def handle_result(scan_df):
            callback(scan_df.result)

        df.add_callback(handle_result)


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

    def last_seen(self, range_id):
        query = copy.copy(self)
        query.args['ExclusiveStartKey'] = {'HashKeyElement': self.args['HashKeyValue'], 'RangeKeyElement': utils.format_value(range_id)}
        return query

    def _query(self):
        defer = Defer(self.table.db.ioloop)

        def handle_result(result_data, error=None):
            results = None
            if error is None:
                if 'ConsumedCapacityUnits' in result_data:
                    self.table._record_read_capacity(float(result_data['ConsumedCapacityUnits']))

                defer.callback(QueryResults(self, result_data))
            else:
                defer.exception(ex=parse_error(error))

        self.table.db.client.make_request('Query', body=json.dumps(self.args), callback=handle_result)
        return defer

    def __call__(self, timeout=None):
        if not self.table.db.allow_sync:
            raise SyncUnallowedError()

        d = self._query()
        args, kwargs =  d(timeout=timeout)
        return args[0]

    def defer(self):
        return self._query()

    def async(self, callback=None):
        self._query(callback=callback)

        def handle_result(q_df):
            callback(q_df.result)

        df.add_callback(handle_result)


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

    def combine(self, other_results):
        return CombinedQueryResults(self, other_results)


class CombinedQueryResults(QueryResults):
    """Special version of QueryResults that puts multiple sets of QueryResults together.

    This is usually constructed by using the combine() method on a QueryResult.
    """
    def __init__(self, query_results, other_query_results):
        self.query = query_results.query
        self.result_data = {}

        self.result_data['Items'] = query_results.result_data['Items'] + other_query_results.result_data['Items']
        self.result_data['Count'] = query_results.result_data['Count'] + other_query_results.result_data['Count']


class SecondaryQueryResults(object):
    def __init__(self, query_results, table, op_results, key):
        self.query_results = query_results
        self.table = table
        self.op_results = op_results
        self.key_func = key

    def combine(self, other_results):
        self.query_results = self.query_results.combine(other_results)
        return self

    def __iter__(self):
        for item in self.query_results:
            key = self.key_func(item)
            op = operation.GetOperation(self.table, key)
            full_item, err = self.op_results[op]
            if err:
                raise err

            yield full_item

    def __len__(self):
        return len(self.query_results)


class ScanResults(Results):
    def __init__(self, scan, result_data):
        self.scan = scan
        self.result_data = result_data

def run_all(runnable):
    while True:
        result = runnable()
        for res in result:
            yield res

        if result.has_next:
            if isinstance(runnable, Scan):
                runnable = runnable.table.scan_next(result)
            else:
                runnable = runnable.table.query_next(result)
        else:
            break

