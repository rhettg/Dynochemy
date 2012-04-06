# -*- coding: utf-8 -*-

"""
Dynochemy
~~~~~~~~

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.

"""

__title__ = 'dynochemy'
__version__ = '0.0.1'
__description__ = 'Clever pythonic and async interface to Amazon DynamoDB '
__url__ = 'https://github.com/rhettg/dynochemy'
__build__ = 0
__author__ = 'Rhett Garber'
__license__ = 'ISC'
__copyright__ = 'Copyright 2012 Rhett Garber'


from . import utils
from .db import DB
from .errors import Error
