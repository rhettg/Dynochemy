# -*- coding: utf-8 -*-

"""
dynochemy.utils
~~~~~~~~

This module provides utility functions that are used within Dynochemy
especially for things like formatting datastructures for Dynamo.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.

"""
import time
import logging
import itertools
import types
import contextlib

from tornado.simple_httpclient import _HTTPConnection
from tornado.httpclient import HTTPResponse
from tornado.ioloop import IOLoop

log = logging.getLogger(__name__)

def stringify(val):
    if isinstance(val, bool):
        return str(int(val))
    return str(val)

def format_value(value):
    if isinstance(value, basestring):
        if not value:
            raise ValueError("empty string")
        return {'S': value}
    elif isinstance(value, (int, long, float, bool)):
        return {'N': stringify(value)}
    elif isinstance(value, (list, tuple, set)):
        all_values = [format_value(v) for v in set(value)]
        spec = None 
        values = []
        for value_spec in all_values:
            if value_spec.keys()[0] == 'S':
                spec = 'SS'
            elif value_spec.keys()[0] == 'N':
                spec = 'NS'
            values.append(value_spec.values()[0])
        return {spec: values}
    else:
        raise ValueError(value)

def _parse_value_spec(type_spec, value):
    if type_spec == 'S':
        return value
    if type_spec == 'N' and '.' in value:
        return float(value)
    elif type_spec == 'N':
        return int(value)
    else:
        raise ValueError(type_spec)

def format_key(key_spec, key_value):
    assert isinstance(key_value, (list, tuple))
    if len(key_spec) == 1:
        return {key_spec[0]: format_value(key_value[0])}
    else:
        out_key = {}
        for key, value in zip(key_spec, key_value):
            out_key[key] = format_value(value)
        return out_key

def parse_key(key):
    parsed_value = parse_item(key)

    if 'RangeKeyElement' in parsed_value:
        return (parsed_value['HashKeyElement'], parsed_value['RangeKeyElement'])
    else:
        return (parsed_value['HashKeyElement'],)

EMPTY_VALUES = ['', None, [], set(), tuple()]
def format_item(item):
    try:
        out = {}
        for k, v in item.iteritems():
            if v in EMPTY_VALUES:
                continue
            out[k] = format_value(v)
    except (TypeError, ValueError), e:
        log.error("Failed to format value for %s", k)
        raise
    return out

def parse_value(value_spec):
    key, value = value_spec.items()[0]
    if len(key) == 1:
        return _parse_value_spec(key, value)
    elif key == 'SS':
        # Note that we could use 'sets' for these, but they are kinda awkward
        # to deal with (and don't natively json encode)
        return value
    elif key == 'NS':
        return [_parse_value_spec('N', v) for v in value]
    else:
        raise ValueError(key)

def parse_item(item):
    return dict((k, parse_value(v)) for k, v in item.iteritems())

def segment(iterable, size):
    it = iter(iterable)
    while True:
        group = tuple(itertools.islice(it, None, size))
        if not group:
            break
        yield group


class ResourceCounter(object):
    """Utility for tracking limits in resource usage over time.

    We use this to track, how much capacity has been used for reading or writing to DynamoDB.

    General usage is to:
        * Create TimeCounter with a size in seconds (how much time we want to track)
        * Set limits over past n seconds with set_limit()
        * Record values as they occur. These will be slotted with 1 second accuracy.
        * Call check() to see if limits have been exceeded. 
        * Call avg() to see what the moving average is over the specified interval.
    """
    def __init__(self, size=60):
        self.size = size
        self.last_second = None
        self.values = []
        self.limits = []

    def avg(self, seconds=1):
        """Returns the moving average over the specified interval"""
        if self.last_second is None:
            return 0.0

        age = int(time.time()) - self.last_second
        interval_left = seconds - age

        if seconds - age > 0:
            value = sum(self.values[0:interval_left]) / float(seconds)
            log.debug("Average over %d: %.1f", seconds, value)
            return value

        return 0.0

    def record(self, value):
        current_time = int(time.time())
        if self.last_second is None:
            self.values.append(0)
        elif self.last_second < current_time:
            while self.last_second < current_time:
                self.values.insert(0, 0)
                self.last_second += 1

        self.values[0] += value
        self.last_second = current_time
        del self.values[self.size:]

    def check(self):
        for seconds, value in self.limits:
            if self.avg(seconds) > value:
                return False

        return True

    def set_limit(self, seconds, value):
        self.limits.append((seconds, value))


def predict_capacity_usage(request_type, args, result=None):
    if request_type == 'GetItem':
        return 1.0
    elif request_type == 'BatchGetItem':
        out = {}
        for table_name, requests in args['RequestItems'].iteritems():
            out[table_name] = 1.0 * len(requests['Keys'])
        return out
    elif request_type == 'PutItem':
        return 1.0
    elif request_type == 'UpdateItem':
        return 1.0
    elif request_type == 'DeleteItem':
        return 1.0
    elif request_type == 'BatchWriteItem':
        out = {}
        for table_name, requests in args['RequestItems'].iteritems():
            out[table_name] = 1.0 * len(requests)
        return out
    elif request_type == 'Query':
        if result:
            return 1.0 * len(result['Items'])
        else:
            return 25.0
    elif request_type == 'Scan':
        if result:
            return 1.0 * len(result['Items'])
        else:
            return 25.0
    else:
        raise ValueError(request_type)


def patch_io_loop():
    """Create an IOLoop sub-class

    We have out own io loop subclass that handles unhandled exceptions by
    re-raising them and quitting.  This is useful for io loops used in contexts
    that are not multi-user... for example dynochemy uses its own ioloops for
    parallelizing io operations, in which case an exception would be better off
    just raised out of ioloop.start()

    This is a function that generates a class because newwer version of tornado
    has some dynamic IOLoop sub-class selection mechanism based on platform. So
    we can just statically subclass 'IOLoop'.
    """
    if hasattr(IOLoop, 'configurable_default'):
        base_cls = IOLoop.configurable_default()
    else:
        base_cls = IOLoop

    class StrictExceptionIOLoop(base_cls):
        def handle_callback_exception(self, callback):
            raise

    return StrictExceptionIOLoop

def patch_http_client():
    """Patch a simple_httpclient instance to raise exceptions generated in callbacks.

    See https://github.com/facebook/tornado/pull/652
    """
    @contextlib.contextmanager
    def better_cleanup(self):
        try:
            yield
        except Exception, e:
            if self.final_callback:
                self._run_callback(HTTPResponse(self.request, 599, error=e,
                                   request_time=time.time() - self.start_time,
                                ))

                if hasattr(self, "stream"):
                    self.stream.close()
            else:
                # If our callback has already been called, we are probably
                # catching an exception that is not caused by us but rather
                # some child of our callback. Rather than drop it on the floor,
                # pass it along.
                raise

    _HTTPConnection.cleanup = types.UnboundMethodType(better_cleanup, None, _HTTPConnection)

