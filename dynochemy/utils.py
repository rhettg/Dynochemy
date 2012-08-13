# -*- coding: utf-8 -*-

"""
dynochemy.utils
~~~~~~~~

This module provides utility functions that are used within Dynochemy
especially for things like formatting datastructures for Dynamo.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.

"""

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
                spec = 'SN'
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

EMPTY_VALUES = ['', None, [], set(), tuple()]
def format_item(item):
    return dict((k, format_value(v)) for k, v in item.iteritems() if v not in EMPTY_VALUES)

def parse_value(value_spec):
    key, value = value_spec.items()[0]
    if len(key) == 1:
        return _parse_value_spec(key, value)
    elif key == 'SS':
        # Note that we could use 'sets' for these, but they are kinda awkward
        # to deal with (and don't natively json encode)
        return value
    elif key == 'SN':
        return [_parse_value_spec('N', v) for v in value]
    else:
        raise ValueError(key)

def parse_item(item):
    return dict((k, parse_value(v)) for k, v in item.iteritems())


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
        self.limits = []

    def set_limit(self, seconds, value):
        pass

    def record(self, value):
        pass

    def check(self):
        pass
    
    def avg(self, seconds=1):
        """Returns the moving average over the specified interval"""
        pass


def predict_capacity_usage(request_type, args):
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
    else:
        raise ValueError(request_type)
