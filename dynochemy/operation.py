# -*- coding: utf-8 -*-
"""
This module contains classes for abstract Dynochemy operations.

This abstraction is a framework for combining multiple operations into sets of
operations that can be exectuted together.

There are a few levels of usage here:

    * Primitive Operations (Get, Put, Delete, Update)
    * Combined Operations (single requests with multiple operations in them: BatchRead, BatchWrite)
    * OperationsSet (operations and combined operations that can be run simulataneously)
    * OperationSequence (a sequence of operation sets that must be run in order. stops on failure)

Note that any instance of 'Operation' can be run individually, but the real
power come from allowing a higher power to manage the execution (see solvent)

Another interesting object is the OperationResult, which provides a way for
results to be store the result of a combined operation, but keyed by primitive
operation.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import functools
import itertools
import copy

from . import errors
from . import defer
from . import utils
from . import constants


class Operation(object):
    """(Abstract)Base class for all operations.

    """

    def run_defer(self, db):
        raise NotImplementedError

    @property
    def unique_key(self):
        raise NotImplementedError

    def __eq__(self, other):
        return self.unique_key == other.unique_key

    def __hash__(self):
        return hash(self.unique_key)

    def run(self, db, op_result=None):
        op_result = op_result or OperationResult()
        df = self.run_defer(db, op_result)
        result, err = df()
        if err:
            raise err
        return result

    def run_async(self, db, op_result=None, callback=None):
        op_result = op_result or OperationResult()
        df = self.run_defer(db, op_result)
        def handle_result(cb):
            if callback is not None:
                callback(*cb.result)

        df.add_callback(handle_result)

    def have_result(self, op_result, op_cb):
        """Called when a result for this operation is available.

        Args -
          op_result: OperationResult object we have been storing results into for this operation.
          value: The value from the operation
          err: The error (may be None)

        This gives an operation the chance to intercept the processing of results, and potentially queue 'next operations'.
        """
        op_result.record_result(self, 
                             cb.result, 
                             read_capacity=cb.kwargs.get('read_capacity'), 
                             write_capacity=cb.kwargs.get('write_capacity'))


def combine_dicts(left_dict, right_dict, combiner=None):
    """Utility function for combining two dictionaries (union) with a user specified 'combiner' function

    Note that the default combiner just takes the value in right first, followed by left if they are truthy
    """
    if combiner is None:
        def combiner(l, r):
            return r or l

    if not any((left_dict, right_dict)):
        return {}

    if not left_dict and right_dict:
        # Swap arguments so we have some value
        s_dict = left_dict
        left_dict = right_dict
        right_dict = {}

    right_dict = right_dict or {}

    out_dict = left_dict.copy()
    for k, v in right_dict.iteritems():
        out_dict[k] = combiner(left_dict.get(k), v)
    return out_dict


class UpdateOperation(Operation):
    def __init__(self, table, key, add=None, put=None, delete=None):
        self.table = table
        self.key = key
        self.add = add
        self.put = put
        self.delete = delete

    def combine_updates(self, update_op):
        """Combine two UpdateOperations assuming they are for the same table/key combination"""
        assert self.table == update_op.table
        assert self.key == update_op.key

        new_update = UpdateOperation(self.table, self.key)

        def add_combiner(l, r):
            if l and r:
                return l + r
            else:
                return l or r

        new_update.add = combine_dicts(self.add, update_op.add, combiner=add_combiner)
        new_update.put = combine_dicts(self.put, update_op.put)
        new_update.delete = combine_dicts(self.delete, update_op.delete)

        return new_update

    def run_defer(self, db, op_result):
        df = OperationResultDefer(op_result, db.ioloop)

        def record_result(cb):
            value, err = cb.result
            self.have_result(op_result, cb)
            df.callback(cb)

        update_df = getattr(db, self.table.__name__).update_defer(self.key, add=self.add, put=self.put, delete=self.delete)
        update_df.add_callback(record_result)

        return df

    @property
    def unique_key(self):
        return ('UPDATE', self.table.name, self.key)


class _WriteBatchableMixin(object):
    """Mixing for operations that can be put in a batch write"""
    def add_to_batch(self, batch):
        raise NotImplementedError


class _ReadBatchableMixin(object):
    """Mixing for operations that can be put in a batch read"""
    def add_to_batch(self, batch):
        raise NotImplementedError


class BatchOperation(Operation):
    __slots__ = ["ops"]
    def __init__(self):
        self.ops = set()

    def __len__(self):
        return len(self.ops)

    def __iter__(self):
        for op in self.ops:
            yield op

    def have_result(self, op_result, op_cb, op=None):
        # This method will be called for each sub-op, and also for the overall
        # operation (where we can rely on base-class functionality)
        if op is None:
            super(BatchOperation, self).have_result(op_result, op_cb)
        else:
            assert op in self.ops

            # Note that we are not passing read/write capacity stuff along here
            # because we don't want to double count
            op_result.record_result(op, cb.result)


class BatchWriteOperation(BatchOperation):
    def add(self, op):
        if not isinstance(op, _WriteBatchableMixin):
            raise ValueError(op)

        if isinstance(op, BatchWriteOperation):
            self.ops.update(op.ops)
        else:
            self.ops.add(op)

    def run_defer(self, db, op_result):
        df = OperationResultDefer(op_result, db.ioloop)
        if not self.ops:
            df.done = True
            return df

        def record_result(op, cb):
            self.have_result(op_result, cb, op=op)

        all_batch_defers = []
        def handle_batch_result(cb):
            result.update_write_capacity(cb.kwargs.get('write_capacity', {}))
            if all(df.done for df in all_batch_defers) and not df.done:
                self.have_result(op_result, cb)
                df.callback(cb)

        for op_set in utils.segment(self.ops, constants.MAX_BATCH_WRITE_ITEMS):
            batch = db.batch_write()
            for op in op_set:
                op_df = op.add_to_batch(batch)
                op_df.add_callback(functools.partial(record_result, op))

            batch_df = batch.defer()
            all_batch_defers.append(batch_df)

        for batch_df in all_batch_defers:
            batch_df.add_callback(handle_batch_result)

        return df


class BatchReadOperation(BatchOperation):
    def add(self, op):
        if not isinstance(op, _ReadBatchableMixin):
            raise ValueError(op)
        if isinstance(op, BatchReadOperation):
            self.ops.update(op.ops)
        else:
            self.ops.add(op)

    def run_defer(self, db, op_result):
        df = OperationResultDefer(op_result, db.ioloop)

        if not self.ops:
            df.done = True
            return df

        all_batch_defers = []
        def handle_batch_result(cb):
            result.update_read_capacity(cb.kwargs.get('read_capacity'))

            if all(df.done for df in all_batch_defers) and not df.done:
                self.have_result(op_result, cb)
                df.callback(cb)

        def handle_op_result(op, cb):
            self.have_result(op_result, cb, op=op)

        for op_set in utils.segment(self.ops, constants.MAX_BATCH_READ_ITEMS):
            batch = db.batch_read()
            for op in op_set:
                op_df = op.add_to_batch(batch)

                # To collect our results per sub-operation, we'll need to add a
                # hook to record our specific part of the response for each
                # sub-op
                op_df.add_callback(functools.partial(handle_op_result, op))

            batch_df = batch.defer()
            all_batch_defers.append(batch_df)

        for batch_df in all_batch_defers:
            batch_df.add_callback(handle_batch_result)

        return df


class PutOperation(Operation, _WriteBatchableMixin):
    def __init__(self, table, entity):
        self.table = table
        self.entity = entity

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, 
                                 cb.result, 
                                 read_capacity=cb.kwargs.get('read_capacity'), 
                                 write_capacity=cb.kwargs.get('write_capacity'))
            df.callback(cb)

        op_df = getattr(db, self.table.__name__).put_defer(self.entity)
        op_df.add_callback(record_result)

        return df

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).put(self.entity)

    @property
    def unique_key(self):
        key = tuple([self.entity[k] for k in self.table(None).key_spec])
        return ('PUT', self.table.name, key)


class DeleteOperation(Operation, _WriteBatchableMixin):
    def __init__(self, table, key):
        self.table = table
        self.key = key

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, 
                                 cb.result, 
                                 read_capacity=cb.kwargs.get('read_capacity'), 
                                 write_capacity=cb.kwargs.get('write_capacity'))
            df.callback(cb)

        op_df = getattr(db, self.table.__name__).delete_defer(self.key)
        op_df.add_callback(record_result)

        return df

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).delete(self.key)

    @property
    def unique_key(self):
        # Assumes updates for the same key have been combined together
        return ('DELETE', self.table.name, self.key)


class GetOperation(Operation, _ReadBatchableMixin):
    def __init__(self, table, key):
        self.table = table
        self.key = key

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, 
                                 cb.result, 
                                 read_capacity=cb.kwargs.get('read_capacity'), 
                                 write_capacity=cb.kwargs.get('write_capacity'))
            df.callback(cb)

        op_df = getattr(db, self.table.__name__).get_defer(self.key)
        op_df.add_callback(record_result)

        return df

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).get(self.key)

    @property
    def unique_key(self):
        # Assumes updates for the same key have been combined together
        return ('GET', self.table.name, self.key)


class QueryOperation(Operation):
    """Combined query operation that runs multiple sub-queries until retieving all the requested results.
   
    What this really means, is that doing multiple individual query requests is
    handled directly by the Operation, rather than by a higher-level solvent,
    which I would prefer. 
    
    This issue is that the results of each
    QuerySegmentOperation need to be combined together intelligently but there
    isn't currently a real clean way for a solvent to sort that out for us.

    This big problem is (besides how complex it is) how we handle errors and
    retries. We could leave provisioning error issues to our 
    solvent, but that means we'll re-run the entire query operation, not just a segment.
    """
    def __init__(self, table, key, args=None):
        self.table = table
        self.hash_key = key
        self.args = args or {}

    def __copy__(self):
        op = QueryOperation(self.table, self.hash_key, args=copy.copy(self.args))
        return op

    def range(self, start=None, end=None):
        self.args['range'] = (start, end)
        return self

    def reverse(self, reverse=True):
        self.args['reverse'] = reverse

    def limit(self, limit):
        self.args['limit'] = limit

    @property
    def unique_key(self):
        # Assumes updates for the same key have been combined together
        return ('QUERY', self.table.name, self.hash_key, tuple(self.args.items()))

    def run_defer(self, db):
        result = QueryOperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        segment_op = QuerySegmentOperation(self.table, self.hash_key, args=copy.copy(self.args))

        def record_result(cb):
            result.record_result(self, 
                                 cb.result, 
                                 read_capacity=cb.kwargs.get('read_capacity'), 
                                 write_capacity=cb.kwargs.get('write_capacity'))

            op_result, err = cb.result
            segment_op, (query_result, err) = list(op_result.iteritems())[0]
            if err or not query_result.has_next:
                df.callback(cb)
            else:
                last_key = utils.parse_key(query_result.result_data['LastEvaluatedKey'])
                next_op = QuerySegmentOperation(self.table, self.hash_key, args=copy.copy(self.args))
                next_op.last_seen(last_key[1])
                next_op_df = next_op.run_defer(db)
                next_op_df.add_callback(record_result)

        op_df = segment_op.run_defer(db)
        op_df.add_callback(record_result)
        return df


class QuerySegmentOperation(Operation):
    def __init__(self, table, key, args=None):
        self.table = table
        self.hash_key = key
        self.args = args or {}

    def __copy__(self):
        op = QuerySegmentOperation(self.table, self.hash_key, args=copy.copy(self.args))
        return op

    def range(self, start=None, end=None):
        self.args['range'] = (start, end)
        return self

    def reverse(self, reverse=True):
        self.args['reverse'] = reverse

    def limit(self, limit):
        self.args['limit'] = limit

    def last_seen(self, range_id):
        self.args['last_seen'] = range_id

    @property
    def unique_key(self):
        # Assumes updates for the same key have been combined together
        return ('QUERY_SEGMENT', self.table.name, self.hash_key, tuple(self.args.items()))

    def run_defer(self, db):
        result = OperationResult(db)
        df = OperationResultDefer(result, db.ioloop)

        def record_result(cb):
            result.record_result(self, 
                                 cb.result, 
                                 read_capacity=cb.kwargs.get('read_capacity'), 
                                 write_capacity=cb.kwargs.get('write_capacity'))
            df.callback(cb)

        query = getattr(db, self.table.__name__).query(self.hash_key)
        for param, arg in self.args.iteritems():
            if param == 'range':
                query = query.range(*arg)
            else:
                query = getattr(query, param)(arg)

        op_df = query.defer()
        op_df.add_callback(record_result)
        return df


class OperationResultDefer(defer.Defer):
    """Special defer that returns a OperationResult, error tuple
    """
    def __init__(self, op_result, io_loop):
        super(OperationResultDefer, self).__init__(io_loop)
        self.op_result = op_result
        self.error = None

    def callback(self, err):
        if err:
            self.error = err

        super(OperationResultDefer, self).callback(None)
        
    @property
    def result(self):
        return self.op_result, self.error


# TODO: Make this just a dict?
class OperationResult(object):
    def __init__(self, db):
        self.db = db
        self.results = {}

        self.read_capacity = {}
        self.write_capacity = {}

        self.next_ops = []
        self.error_attempts = 0

    def record_result(self, op, result, read_capacity=None, write_capacity=None):
        self.results[op] = result

        if read_capacity:
            self.update_read_capacity(read_capacity)

        if write_capacity:
            self.update_write_capacity(write_capacity)

        next_ops = op.result(result)
        if next_ops:
            self.next_ops += next_ops

        return next_ops

    def rethrow(self):
        for op, (_, err) in self.iteritems():
            if err:
                log.info("Failed Operation %r: %r", op, err)
                raise err

    def update(self, other_result):
        assert self.db == other_result.db
        self.results.update(other_result.results)

        self.update_read_capacity(other_result.read_capacity)
        self.update_write_capacity(other_result.write_capacity)

        self.next_ops += other_result.next_ops

    def update_read_capacity(self, read_capacity):
        for name, value in read_capacity.iteritems():
            self.read_capacity.setdefault(name, 0.0)
            self.read_capacity[name] += value

    def update_write_capacity(self, write_capacity):
        for name, value in write_capacity.iteritems():
            self.write_capacity.setdefault(name, 0.0)
            self.write_capacity[name] += value

    def iteritems(self):
        return self.results.iteritems()

    def __getitem__(self, key):
        return self.results[key]

    def __repr__(self):
        return repr(self.results)


class QueryOperationResult(OperationResult):
    """Special version of OperationResult which understands that we'll want a
    combined query result for QueryOperation (made up of possibly sereral
    QuerySegmentOperations)
    """
    def record_result(self, op, result, read_capacity=None, write_capacity=None):
        if op in self.results:
            new_result, _ = result
            old_result, _ = self.results[op]

            new_query_result, err = new_result.results.values()[0]
            assert not err

            old_query_result, err = old_result.results.values()[0]
            assert not err

            combined_results = old_query_result.combine(new_query_result)
            return super(QueryOperationResult, self).record_result(op, (combined_results, None), read_capacity, write_capacity)
        else:
            return super(QueryOperationResult, self).record_result(op, result, read_capacity, write_capacity)


__all__ = ["GetOperation", "PutOperation", "DeleteOperation", "UpdateOperation", "QueryOperation", "OperationSet"]
