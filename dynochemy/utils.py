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
