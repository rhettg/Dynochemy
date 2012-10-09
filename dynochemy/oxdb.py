# -*- coding: utf-8 -*-
"""
Instrumented Dynochemy Database
We use 'BlueOx' to provide useful stats and reporting for requests to dynochemy

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import logging
import time
import json

from . import db
from . import sql

# Don't fail if blueox isn't supported.
try:
    import blueox
except ImportError:
    blueox = None


class OxClient(object):
    def __init__(self, client):
        self.real_client = client

    def register_table(self, *args, **kwargs):
        return self.real_client.register_table(*args, **kwargs)

    def make_request(self, command, body=None, callback=None):
        start_time = time.time()

        def wrapped_callback(*args, **kwargs):
            with blueox.Context('.dynochemy') as ctx:
                # Rewrite our start time, this is async, remember
                ctx.start_time = start_time
                ctx.set('command', command)

                resp = args[0]
                err = kwargs.get('error')

                if 'Responses' in resp:
                    for table_name, table_resp in resp['Responses'].iteritems():
                        if 'ConsumedCapacityUnits' in table_resp:
                            ctx.set('tables.%s.consumed_capacity' % table_name, table_resp['ConsumedCapacityUnits'])
                elif 'ConsumedCapacityUnits' in resp:
                    req = json.loads(body)
                    ctx.set('tables.%s.consumed_capacity' % req.get('TableName', ''), resp['ConsumedCapacityUnits'])

                if 'Count' in resp:
                    ctx.set('count', resp['Count'])

                if err:
                    ctx.set('error', str(err))

            if callback:
                callback(*args, **kwargs)

        return self.real_client.make_request(command, body=body, callback=wrapped_callback)


class OxDB(db.DB):
    def __init__(self, access_key, access_secret, ioloop=None):
        super(OxDB, self).__init__(access_key, access_secret, ioloop=ioloop)

        # Wrap the client
        self.client = OxClient(self.client)


class OxSQLDB(sql.SQLDB):
    def __init__(self, engine):
        super(OxSQLDB, self).__init__(engine)

        # Wrap the client
        self.client = OxClient(self.client)
