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


def compare(operator, lval, rval):
    if operator == 'EQ':
        return lval == rval
    elif operator == 'GT':
        return lval > rval
    elif operator == 'LT':
        return lval < rval
    else:
        raise NotImplementedError(operator)


class SQLClient(object):
    def __init__(self, engine, name, key_spec):
        self.engine = engine
        self.key_spec = key_spec

        self.table = metadata.tables[name]

    def do_getitem(self, args):
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

    def do_putitem(self, args):
        item_data = args['Item']
        args = utils.parse_item(item_data)
        if len(self.key_spec) == 2:
            hash_key = args[self.key_spec[0]]
            range_key = args[self.key_spec[1]]
        else:
            raise NotImplementedError

        ins = self.table.insert().values(hash_key=hash_key, range_key=range_key, content=json.dumps(item_data))
        self.engine.execute(ins)
        return None

    def do_scan(self, args):
        q = sql.select([self.table])
        if 'Limit' in args:
            q = q.limit(args['Limit'])

        unsupported_keys = set(args.keys()) - set(['Limit', 'TableName', 'ScanFilter']) 
        if unsupported_keys:
            raise NotImplementedError(unsupported_keys)

        out = {'Items': []}
        for res in self.engine.execute(q):
            item_data = json.loads(res[self.table.c.content])
            item = utils.parse_item(item_data)
            if 'ScanFilter' in args:
                for attribute, filter_spec in args['ScanFilter'].iteritems():
                    if attribute not in item:
                        continue
                    for value_spec in filter_spec['AttributeValueList']:
                        value = utils.parse_value(value_spec)
                        if compare(filter_spec['ComparisonOperator'], item[attribute], value):
                            out['Items'].append(item_data)
                            break
            else:
                out['Items'].append(item_data)


        return out

    def do_query(self, args):
        pprint.pprint(args)

        unsupported_keys = set(args.keys()) - set(['Limit', 'TableName', 'HashKeyValue', 'ScanIndexForward', 'ConsistentRead']) 
        if unsupported_keys:
            raise NotImplementedError(unsupported_keys)

        if len(self.key_spec) < 2:
            raise NotImplementedError

        expr = self.table.c.hash_key == utils.parse_value(args['HashKeyValue'])
        q = sql.select([self.table], expr)
        if 'Limit' in args:
            q = q.limit(args['Limit'])
        
        if args.get('ScanIndexForward', True):
            q = q.order_by(self.table.c.range_key.asc())
        else:
            q = q.order_by(self.table.c.range_key.desc())

        out = {'Items': []}
        for res in self.engine.execute(q):
            item_data = json.loads(res[self.table.c.content])
            item = utils.parse_item(item_data)
            out['Items'].append(item_data)

        return out

    def make_request(self, command, body=None, callback=None):
        result = None
        try:
            result = getattr(self, "do_%s" % command.lower())(json.loads(body))
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

    db[('Britt', 'A')] = {'last_name': 'Deal', 'value': 9}
    db[('Britt', 'B')] = {'last_name': 'Deal', 'value': 9}
    db[('Britt', 'C')] = {'last_name': 'Deal', 'value': 9}

    #print db[('Britt', 'A')]

    #s = db.scan().limit(2) #.filter_gt('value', 10)
    #for r in s():
        #print r

    q = db.query('Britt').limit(2)
    for r in q():
        print r

    #print db[('Rhett', 'A')]

