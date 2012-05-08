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


def create_db(db_name, metadata, has_range_key=True):
    if has_range_key:
        dynamo_tbl = Table(db_name, metadata,
                           Column('hash_key', String, primary_key=True),
                           Column('range_key', String, primary_key=True),
                           Column('content', LargeBinary),
                        )
    else:
        dynamo_tbl = Table(db_name, metadata,
                           Column('hash_key', String, primary_key=True),
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

        metadata = sqlalchemy.MetaData(bind=engine)

        try:
            self.table = Table(name, metadata, autoload=True, autoload_with=self.engine)
        except sqlalchemy.exc.NoSuchTableError:
            self.table = create_db(name, metadata, has_range_key=bool(len(key_spec) > 1))
            self.table.create(engine)

    def do_getitem(self, args):
        if len(self.key_spec) > 1:
            expr = sql.and_(self.table.c.hash_key == utils.parse_value(args['Key']['HashKeyElement']), 
                              self.table.c.range_key == utils.parse_value(args['Key']['RangeKeyElement']))
        else:
            expr = self.table.c.hash_key == utils.parse_value(args['Key']['HashKeyElement'])

        q = sql.select([self.table], expr)
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
        hash_key = args[self.key_spec[0]]
        if len(self.key_spec) == 2:
            range_key = args[self.key_spec[1]]
            ins = self.table.insert().values(hash_key=hash_key, range_key=range_key, content=json.dumps(item_data))
        else:
            ins = self.table.insert().values(hash_key=hash_key, content=json.dumps(item_data))

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
        unsupported_keys = set(args.keys()) - set(['Limit', 'TableName', 'HashKeyValue', 'ScanIndexForward', 'ConsistentRead', 'RangeKeyCondition', 'AttributesToGet']) 
        if unsupported_keys:
            raise NotImplementedError(unsupported_keys)


        scan_forward = args.get('ScanIndexForward', True)

        expr = self.table.c.hash_key == utils.parse_value(args['HashKeyValue'])

        if 'RangeKeyCondition' in args:
            if len(self.key_spec) < 2:
                raise NotImplementedError

            operator = args['RangeKeyCondition']['ComparisonOperator']
            if operator == 'BETWEEN':
                start = utils.parse_value(args['RangeKeyCondition']['AttributeValueList'][0])
                end = utils.parse_value(args['RangeKeyCondition']['AttributeValueList'][1])
                expr = sql.and_(expr, self.table.c.range_key.between(start, end))

            else:
                range_value = utils.parse_value(args['RangeKeyCondition']['AttributeValueList'][0])
                if operator == 'GT':
                    expr = sql.and_(expr, self.table.c.range_key > range_value)
                elif operator == 'LT':
                    expr = sql.and_(expr, self.table.c.range_key < range_value)
                elif operator == 'GE':
                    expr = sql.and_(expr, self.table.c.range_key >= range_value)
                elif operator == 'LE':
                    expr = sql.and_(expr, self.table.c.range_key <= range_value)
                elif operator == 'EQ':
                    expr = sql.and_(expr, self.table.c.range_key == range_value)
                elif operator == 'BEGINS_WITH':
                    expr = sql.and_(expr, self.table.c.range_key.like('%s%%' % range_value))
                else:
                    raise NotImplementedError


        q = sql.select([self.table], expr)
        if 'Limit' in args:
            q = q.limit(args['Limit'])

        if len(self.key_spec) > 1:
            if scan_forward:
                q = q.order_by(self.table.c.range_key.asc())
            else:
                q = q.order_by(self.table.c.range_key.desc())

        out = {'Items': []}
        for res in self.engine.execute(q):
            item_data = json.loads(res[self.table.c.content])
            item = utils.parse_item(item_data)
            if 'AttributesToGet' in args:
                out_item = dict((col_name, item_data[col_name]) for col_name in args['AttributesToGet'])
                out['Items'].append(out_item)
            else:
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
    engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=True)

    db = SQLDB(engine, 'RhettTest', ('user',))

    db['Britt'] = {'last_name': 'Deal', 'value': 9}
    db['Rhett'] = {'last_name': 'Rhett', 'value': 10}

    print db['Britt']

    #s = db.scan().limit(2) #.filter_gt('value', 10)
    #for r in s():
        #print r

    q = db.query('Britt').reverse().attributes(['user', 'value']).defer()
    res, error = q()
    for r in res:
        print r

    #print db[('Rhett', 'A')]

