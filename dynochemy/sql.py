# -*- coding: utf-8 -*-
"""
Potential SQLite backing for Dynamo. 
This would be useful for local development

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import json
import pprint

import sqlalchemy
from sqlalchemy import sql, Table, Column, String, LargeBinary

from dynochemy import db
from dynochemy import errors
from dynochemy import utils

metadata = sqlalchemy.MetaData()

def create_db(db_name):
    dynamo_tbl = Table(db_name, metadata,
                       Column('hash_key', String, primary_key=True),
                       Column('range_key', String, primary_key=True),
                       Column('content', LargeBinary),
                    )

    return dynamo_tbl


class CommandError(errors.Error): pass


class SQLClient(object):
    def __init__(self, engine, name, key_spec):
        self.engine = engine
        self.key_spec = key_spec

        self.table = metadata.tables[name]

    def do_getitem(self, body):
        args = json.loads(body)
        filter = sql.and_(self.table.c.hash_key == utils.parse_value(args['Key']['HashKeyElement']), 
                          self.table.c.range_key == utils.parse_value(args['Key']['RangeKeyElement']))

        q = sql.select([self.table], filter)
        try:
            res = list(self.engine.execute(q))[0]
        except IndexError:
            out = {}
        else:
            out = {
                'Item': json.loads(res[self.table.c.content])
            }


        return out

    def do_putitem(self, body):
        item_data = json.loads(body)['Item']
        args = utils.parse_item(item_data)
        if len(self.key_spec) == 2:
            hash_key = args[self.key_spec[0]]
            range_key = args[self.key_spec[1]]
        else:
            raise NotImplementedError

        ins = self.table.insert().values(hash_key=hash_key, range_key=range_key, content=json.dumps(item_data))
        self.engine.execute(ins)
        return None

    def make_request(self, command, body=None, callback=None):
        result = None
        try:
            result = getattr(self, "do_%s" % command.lower())(body)
        except CommandError, e:
            callback(None, error=e.args[0])
            return
        else:
            callback(result, error=None)


class SQLDB(db.BaseDB):
    def __init__(self, engine, name, key_spec):
        super(SQLDB, self).__init__(name, key_spec)
        self.client = SQLClient(engine, name, key_spec)

if __name__  == '__main__':

    tbl = create_db('RhettTest')

    engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=True)
    metadata.create_all(engine)

    db = SQLDB(engine, 'RhettTest', ('user', 'id'))

    db[('Britt', 'A')] = {'last_name': 'Deal'}

    print db[('Britt', 'A')]

    #s = db.scan().limit(2).filter_gt('value', 8)
    #for r in s():
        #print r

    #print db[('Rhett', 'A')]

