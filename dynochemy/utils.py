# -*- coding: utf-8 -*-

"""
bootstrap.util
~~~~~~~~

This module provides utility functions that are used within Bootstrap
that are also useful for external consumption.

:copyright: (c) 2012 by Firstname Lastname.
:license: ISC, see LICENSE for more details.

"""

def build_key(key_spec, key_value):
    if len(key_spec) == 1:
        return {key_spec[0]: _format_value_spec(key_value)}
    else:
        out_key = {}
        for key, value in zip(key_spec, key_value):
            out_key[key] = _format_value_spec(value)
        return out_key

def _format_value_spec(value):
    if isinstance(value, basestring):
        return {'S': value}
    elif isinstance(value, (int, float)):
        return {'N': str(value)}
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

def parse_value(value_spec):
    key, value = value_spec.items()[0]
    if len(key) == 1:
        return _parse_value_spec(key, value)
    else:
        out = []
        for type_char, sub_value in zip(list(key), list(value)):
            out.append(_parse_value_spec(type_char, sub_value))
        return out

