# -*- coding: utf-8 -*-

"""
dynochemy.utils
~~~~~~~~

This module provides utility functions that are used within Dynochemy
especially for things like formatting datastructures for Dynamo.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.

"""

def format_value(value):
    if isinstance(value, basestring):
        return {'S': value}
    elif isinstance(value, (int, float)):
        return {'N': str(value)}
    elif isinstance(list, tuple):
        all_values = [format_value(v) for v in value]
        spec = ""
        values = []
        for value_spec in all_values:
            spec.append(value_spec.keys()[0])
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
    if len(key_spec) == 1:
        return {key_spec[0]: format_value(key_value)}
    else:
        out_key = {}
        for key, value in zip(key_spec, key_value):
            out_key[key] = format_value(value)
        return out_key

def format_item(item):
    return dict((k, format_value(v)) for k, v in item.iteritems())

def parse_value(value_spec):
    key, value = value_spec.items()[0]
    if len(key) == 1:
        return _parse_value_spec(key, value)
    else:
        out = []
        for type_char, sub_value in zip(list(key), list(value)):
            out.append(_parse_value_spec(type_char, sub_value))
        return out

def parse_item(item):
    return dict((k, parse_value(v)) for k, v in item.iteritems())
