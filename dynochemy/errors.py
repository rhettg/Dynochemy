# -*- coding: utf-8 -*-

"""
This module contains the set of Dynochemy's exceptions

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.

"""
import json


class Error(Exception):
    """This is an ambiguous error that occured."""
    pass

class SyncUnallowedError(Error): pass

class DuplicateBatchItemError(Error): pass

class IncompleteSolventError(Error): pass

class ExceededBatchRequestsError(Error): pass

class ItemNotFoundError(Error): pass

class DynamoDBError(Error): pass

class ProvisionedThroughputError(DynamoDBError): pass

class UnprocessedItemError(DynamoDBError): pass



def parse_error(raw_error):
    """Parse the error we get out of Boto into something we can code around"""
    if isinstance(raw_error, Error):
        return raw_error

    if 'ProvisionedThroughputExceededException' in raw_error.error_code:
        return ProvisionedThroughputError(raw_error.error_message)
    else:
        return DynamoDBError(raw_error.error_message, raw_error.error_code)


__all__ = ["Error", "SyncUnallowedError", "DuplicateBatchItemError", "DynamoDBError", "ProvisionedThroughputError", "ItemNotFoundError"]
