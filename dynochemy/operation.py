# -*- coding: utf-8 -*-
"""
This module contains classes for abstract Dynochemy operations.

There are a few levels of usage here:

    * Primitive Operations (Get, Put, Delete, Update)
    * Combined Operations (single requests with multiple operations in them: BatchRead, BatchWrite)
    * Operation sequences (Batches with too many operations to execute at once, Queries and Scans)

Note that any instance of 'Operation' can be run individually, but the real
power come from allowing a higher power to manage the execution (see solvent)

Another important object is the OperationResult, which provides a way for
results to be store the result of a combined operation, but keyed by primitive
operation. To use an operation directly, an OperationResult object must be provided.

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

    def run_defer(self, op_results):
        raise NotImplementedError

    @property
    def unique_key(self):
        raise NotImplementedError

    def __eq__(self, other):
        return self.unique_key == other.unique_key

    def __hash__(self):
        return hash(self.unique_key)

    def run(self, op_results):
        # We require passing in an OperationResult object which might seem
        # weird.  This is one reason why Operation should really be considered
        # an internal API... or at the very least, should probalby never be
        # 'run()' directly.  In an earlier design, we provided the
        # OperationResult for each operation, and then the Solvent() (the more
        # common interface) took care of combining all these results together.
        # However, this proved to be really hard to understand and debug.

        # So in the interest of our sanity, an operation is provied the object
        # it will deliver it's results through.

        df = self.run_defer(op_results)
        result, err = df()
        if err:
            raise err
        return result

    def run_async(self, op_results, callback=None):
        df = self.run_defer(op_results)

        def handle_result(cb):
            if callback is not None:
                callback(*cb.result)

        df.add_callback(handle_result)

    def have_result(self, op_results, op_cb, ignore_capacity=False):
        """Called when a result for this operation is available.

        Args -
          op_result: OperationResult object we have been storing results into for this operation.
          op_cb: The defer making the callback
          ignore_capacity: Set to true if this result should not record capacity. This would be the case if this operation wasn't run individually.

        This gives an operation the chance to intercept the processing of results, and potentially queue 'next operations'.
        """
        if ignore_capacity:
            op_results.record_result(self, op_cb.result)
        else:
            op_results.record_result(self, 
                             op_cb.result, 
                             read_capacity=op_cb.kwargs.get('read_capacity'), 
                             write_capacity=op_cb.kwargs.get('write_capacity'))


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

    def run_defer(self, op_results):
        update_df = getattr(op_results.db, self.table.__name__).update_defer(self.key, add=self.add, put=self.put, delete=self.delete)
        update_df.add_callback(functools.partial(self.have_result, op_results))

        return update_df

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
    # BatchOperations have a slightly more complex design as they need to track sub-operations.
    # We have a few requiremets:
    #   1. Report results on an individual op basis (a defer for each sub-op)
    #   2. Generate follow on batches for sub-ops that didn't get included due to being over capacity

    __slots__ = ["ops"]
    def __init__(self, ops=None):
        self.ops = set()

        if ops:
            for op in ops:
                self.add(op)

    def add(self, op):
        self.ops.add(op)

    def __len__(self):
        return len(self.ops)

    def __iter__(self):
        for op in self.ops:
            yield op


class BatchWriteOperation(BatchOperation):
    def add(self, op):
        if not isinstance(op, _WriteBatchableMixin):
            raise ValueError(op)

        if isinstance(op, BatchWriteOperation):
            self.ops.update(op.ops)
        else:
            self.ops.add(op)

    def run_defer(self, op_results):
        if not self.ops:
            df = defer.Defer()
            df.done = True
            return df

        def handle_op_result(op, cb):
            op.have_result(op_results, cb, ignore_capacity=True)

        def handle_batch_result(cb):
            # Note that we are not recording the full result for this op.
            # This means that in the case of an error, there will be errors for each sub-op, but not
            # one for the overall batch.
            # We do however need to record the capacity used, which we can only do in aggregate.
            if 'write_capacity' in cb.kwargs:
                op_results.update_write_capacity(cb.kwargs['write_capacity'])

        # We might have more operations than we can put in single batch, so prepare ourselves.
        all_ops = list(self.ops)
        batch = op_results.db.batch_write()

        while all_ops:
            op = all_ops.pop()
            try:
                op_df = op.add_to_batch(batch)
            except errors.ExceededBatchRequestsError:
                # Too many requests, put it back
                all_ops.append(op)
                break
            else:
                # To collect our results per sub-operation, we'll need to add a
                # hook to record our specific part of the response for each
                # sub-op
                op_df.add_callback(functools.partial(handle_op_result, op))

        batch_df = batch.defer()
        batch_df.add_callback(handle_batch_result)

        # If there are more batches left, leave them for the next round.
        if all_ops:
            op_results.next_ops += all_ops

        return batch_df


class BatchReadOperation(BatchOperation):
    def add(self, op):
        if not isinstance(op, _ReadBatchableMixin):
            raise ValueError(op)
        if isinstance(op, BatchReadOperation):
            self.ops.update(op.ops)
        else:
            self.ops.add(op)

    def run_defer(self, op_results):
        # The implementation here is very similiar to BatchRead, so you should
        # probalby reference that for more explanation.
        if not self.ops:
            df = defer.Defer()
            df.done = True
            return df

        def handle_op_result(op, cb):
            op.have_result(op_results, cb)

        def handle_batch_result(cb):
            op_results.update_read_capacity(cb.kwargs.get('read_capacity', {}))

        all_ops = list(self.ops)

        batch = op_results.db.batch_read()
        while all_ops:
            op = all_ops.pop()
            try:
                op_df = op.add_to_batch(batch)
            except errors.ExceededBatchRequestsError:
                all_ops.append(op)
                break
            else:
                op_df.add_callback(functools.partial(handle_op_result, op))

        if all_ops:
            op_results.next_ops += all_ops

        batch_df = batch.defer()
        batch_df.add_callback(handle_batch_result)

        return batch_df


class PutOperation(Operation, _WriteBatchableMixin):
    def __init__(self, table, entity):
        self.table = table
        self.entity = entity

    def run_defer(self, op_results):
        op_df = getattr(op_results.db, self.table.__name__).put_defer(self.entity)
        op_df.add_callback(functools.partial(self.have_result, op_results))

        return op_df

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

    def run_defer(self, op_results):
        op_df = getattr(op_results.db, self.table.__name__).delete_defer(self.key)
        op_df.add_callback(functools.partial(self.have_result, op_results))

        return op_df

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

    def run_defer(self, op_results):
        op_df = getattr(op_results.db, self.table.__name__).get_defer(self.key)
        op_df.add_callback(functools.partial(self.have_result, op_results))

        return op_df

    def add_to_batch(self, batch):
        return getattr(batch, self.table.__name__).get(self.key)

    @property
    def unique_key(self):
        # Assumes updates for the same key have been combined together
        return ('GET', self.table.name, self.key)


class GetAndDeleteOperation(GetOperation):
    """Operation that does a Get, then a Delete

    This is a replacement for doing just a straight Delete. First it does a Get, then it schedules a Delete to go afterwards.
    This is useful in solvents because for views, to remove an entity, you really need to know what the entity is.
    """
    def have_result(self, op_results, op_cb):
        super(GetAndDeleteOperation, self).have_result(op_results, op_cb)

        entity, err = op_results[self]
        if not err:
            op_results.next_ops.append(DeleteOperation(self.table, self.key))


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
        op = self.__class__(self.table, self.hash_key, args=copy.copy(self.args))
        return op

    def range(self, start=None, end=None):
        self.args['range'] = (start, end)
        return self

    def reverse(self, reverse=True):
        self.args['reverse'] = reverse
        return self

    def limit(self, limit):
        self.args['limit'] = limit
        return self

    def last_seen(self, range_id):
        self.args['last_seen'] = range_id
        return self

    @property
    def unique_key(self):
        # We are not including the 'last_seen' argument in our hash because we
        # want all the different segments of our query to combine together.
        return ('QUERY', self.table.name, self.hash_key, tuple((k, v) for k, v in self.args.iteritems() if k != 'last_seen'))

    def have_result(self, op_results, op_cb, **kwargs):
        # Query result handling is special.
        # Queries can happen in multiple operations.
        # We need to potentially update our resulting QueryResult object with new entries in place.
        # Also, we might need to queue up our operation again with different bounds if there are more results available.
        new_query_result, new_err = op_cb.result

        if self in op_results:
            # We already have part of this query, update our results in place.
            query_result, err = op_results[self]
            if new_err:
                # Mark us as an error
                op_results.record_result(self, (query_result, new_err))
            else:
                op_results.record_result(self, 
                                         (query_result.combine(new_query_result), new_err), 
                                         read_capacity=op_cb.kwargs.get('read_capacity'))
        else:
            super(QueryOperation, self).have_result(op_results, op_cb)

        if new_query_result.has_next:
            last_key = utils.parse_key(new_query_result.result_data['LastEvaluatedKey'])
            next_op = copy.copy(self)
            next_op.last_seen(last_key[1])
            op_results.next_ops.append(next_op)

    def run_defer(self, op_results):
        query = getattr(op_results.db, self.table.__name__).query(self.hash_key)
        for param, arg in self.args.iteritems():
            if param == 'range':
                query = query.range(*arg)
            else:
                query = getattr(query, param)(arg)

        op_df = query.defer()
        op_df.add_callback(functools.partial(self.have_result, op_results))
        return op_df


class QueryAndDeleteOperation(QueryOperation):
    """An operation that does a query and deletes all the results.

    You can provide an additional filter function (filter_func) that will be
    applied as a filter to all the results from the query.  If the filter
    returns False, the entity will not be deleted.
    """
    def __init__(self, table, key, args=None, filter_func=None):
        super(QueryAndDeleteOperation, self).__init__(table, key, args=args)
        self.filter_func = filter_func

    def __copy__(self):
        op = self.__class__(self.table, self.hash_key, args=copy.copy(self.args), filter_func=self.filter_func)
        return op

    def have_result(self, op_results, op_cb, **kwargs):
        super(QueryAndDeleteOperation, self).have_result(op_results, op_cb, **kwargs)

        filter_func = self.filter_func or (lambda e: True)

        query_result, new_err = op_cb.result
        for res in filter(filter_func, query_result):
            op_results.next_ops.append(DeleteOperation(self.table, (res[self.table.hash_key], res[self.table.range_key])))


class OperationResultDefer(defer.Defer):
    """Special defer that is bound to a result before it's completed.

    The only 'result' that can come from the normal 'callback()' flow is in the case of errors.
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


class OperationResult(object):
    def __init__(self, db):
        self.db = db
        self.results = {}
        self.callbacks = []

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

        # Inform our result callbacks
        [func(op) for func in self.callbacks]

    def rethrow(self):
        for op, (_, err) in self.iteritems():
            if err:
                log.info("Failed Operation %r: %r", op, err)
                raise err

    def add_callback(self, func):
        """Add a function to call when we get a result.

        Argument will be an operation instance.
        Generally the caller should know the result object and can look up the result itself.
        """
        self.callbacks.append(func)

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

    def __iter__(self):
        return iter(self.results)

    def __getitem__(self, key):
        return self.results[key]

    def __repr__(self):
        return repr(self.results)


__all__ = ["GetOperation", "PutOperation", "DeleteOperation", "UpdateOperation", "QueryOperation", "OperationResult"]
