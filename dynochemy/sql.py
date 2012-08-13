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

DEFAULT_LIMIT = 100

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
    def __init__(self, engine):
        self.engine = engine

        self.tables = {}
        self.metadata = sqlalchemy.MetaData(bind=engine)

    def register_table(self, table_cls):
        try:
            sql_table = Table(table_cls.name, self.metadata, autoload=True, autoload_with=self.engine)
        except sqlalchemy.exc.NoSuchTableError:
            sql_table = create_db(table_cls.name, self.metadata, has_range_key=bool(table_cls.range_key))
            sql_table.create(self.engine)

        if table_cls.range_key:
            key_spec = (table_cls.hash_key, table_cls.range_key)
        else:
            key_spec = (table_cls.hash_key,)

        self.tables[table_cls.name] = {
            'sql_table': sql_table,
            'table_def': table_cls,
            'key_spec': key_spec
        }

    def _get_item(self, table_name, key):
        table_spec = self.tables[table_name]
        sql_table = table_spec['sql_table']
        table_def = table_spec['table_def']

        if table_def.range_key:
            expr = sql.and_(sql_table.c.hash_key == utils.parse_value(key['HashKeyElement']), 
                              sql_table.c.range_key == utils.parse_value(key['RangeKeyElement']))
        else:
            expr = sql_table.c.hash_key == utils.parse_value(key['HashKeyElement'])

        q = sql.select([sql_table], expr)
        try:
            res = list(self.engine.execute(q))[0]
        except IndexError:
            return None
        else:
            return json.loads(res[sql_table.c.content])

    def do_getitem(self, args):
        item = self._get_item(args['TableName'], args['Key'])

        if item:
            out = {
                'Item': item
            }
        else:
            out = {}

        return out

    def _put_item(self, table_name, item_data):
        table_spec = self.tables[table_name]
        sql_table = table_spec['sql_table']
        table_def = table_spec['table_def']
        key_spec = table_spec['key_spec']

        item = utils.parse_item(item_data)
        hash_key = item[key_spec[0]]
        if table_def.range_key:
            range_key = item[key_spec[1]]
            del_q = sql_table.delete().where(sql.and_(sql_table.c.hash_key==hash_key, sql_table.c.range_key==range_key))
            ins = sql_table.insert().values(hash_key=hash_key, range_key=range_key, content=json.dumps(item_data))
        else:
            del_q = sql_table.delete().where(sql_table.c.hash_key==hash_key)
            ins = sql_table.insert().values(hash_key=hash_key, content=json.dumps(item_data))

        try:
            self.engine.execute(ins)
        except sqlalchemy.exc.IntegrityError:
            # Try again
            self.engine.execute(del_q)
            self.engine.execute(ins)

    def do_putitem(self, args):
        item_data = args['Item']
        if 'Expected' in args:
            raise NotImplementedError

        self._put_item(args['TableName'], item_data)

    def do_deleteitem(self, args):
        table_spec = self.tables[args['TableName']]
        sql_table = table_spec['sql_table']
        table_def = table_spec['table_def']

        if table_def.range_key:
            expr = sql.and_(sql_table.c.hash_key == utils.parse_value(args['Key']['HashKeyElement']), 
                              sql_table.c.range_key == utils.parse_value(args['Key']['RangeKeyElement']))
        else:
            expr = sql_table.c.hash_key == utils.parse_value(args['Key']['HashKeyElement'])

        item = self.do_getitem(args)

        del_q = sql_table.delete().where(expr)
        res = self.engine.execute(del_q)

        out = {
            'Attributes': item['Item']
        }

        return out

    def do_updateitem(self, args):
        table_spec = self.tables[args['TableName']]
        sql_table = table_spec['sql_table']
        key_spec = table_spec['key_spec']
        table_def = table_spec['table_def']

        if 'Expected' in args:
            raise NotImplementedError

        if args.get('ReturnValues') not in (None, 'ALL_OLD', 'ALL_NEW'):
            raise NotImplementedError

        if table_def.range_key:
            key = (utils.parse_value(args['Key']['HashKeyElement']), 
                   utils.parse_value(args['Key']['RangeKeyElement']))
            expr = sql.and_(sql_table.c.hash_key == key[0],
                              sql_table.c.range_key == key[1])
        else:
            key = (utils.parse_value(args['Key']['HashKeyElement']),)
            expr = sql_table.c.hash_key == key[0]

        q = sql.select([sql_table], expr)
        res = list(self.engine.execute(q))
        if res:
            item = json.loads(res[0][sql_table.c.content])
        else:
            item = {}
            item.update(utils.format_key(key_spec, key))

        real_item = utils.parse_item(item)

        # Apply our updates
        for attribute, value_update in args['AttributeUpdates'].iteritems():
            if value_update['Action'] == "ADD":
                if attribute in real_item:
                    if isinstance(real_item[attribute], (int,float)):
                        real_item[attribute] += utils.parse_value(value_update['Value'])
                    else:
                        real_item[attribute].append(utils.parse_value(value_update['Value']))
                else:
                    real_item[attribute] = utils.parse_value(value_update['Value'])

            elif value_update['Action'] == "PUT":
                real_item[attribute] = utils.parse_value(value_update['Value'])
            elif value_update['Action'] == "DELETE":
                if attribute in real_item:
                    del real_item[attribute]
            else:
                raise ValueError(value_update['Action'])
        
        # write to the db
        self._put_item(args['TableName'], utils.format_item(real_item))

        if args.get('ReturnValues', 'NONE') == 'NONE':
            return {}
        elif args['ReturnValues'] == 'ALL_NEW':
            return {'Attributes': utils.format_item(real_item)}
        elif args['ReturnValues'] == 'ALL_OLD':
            return {'Attributes': item}

    def do_batchwriteitem(self, args):
        for table_name, requests in args['RequestItems'].iteritems():
            for request in requests:
                req_type = request.keys()[0]
                args = request[req_type]
                args['TableName'] = table_name

                if req_type == "PutRequest":
                    self._put_item(table_name, args['Item'])
                elif req_type == "DeleteRequest":
                    self.do_deleteitem(args)
                else:
                    raise NotImplementedError

        return {}

    def do_batchgetitem(self, args):
        out = {}
        for table_name, requests in args['RequestItems'].iteritems():
            out.setdefault(table_name, {'Items': []})
            for key in requests['Keys']:
                # TODO: This would probably be nice as a better multi-key query
                item = self._get_item(table_name, key)
                if item:
                    out[table_name]['Items'].append(item)

        return out

    def do_scan(self, args):
        table_spec = self.tables[args['TableName']]
        sql_table = table_spec['sql_table']

        q = sql.select([sql_table])
        if 'Limit' in args:
            q = q.limit(args['Limit'])

        unsupported_keys = set(args.keys()) - set(['Limit', 'TableName', 'ScanFilter']) 
        if unsupported_keys:
            raise NotImplementedError(unsupported_keys)

        out = {'Items': []}
        for res in self.engine.execute(q):
            item_data = json.loads(res[sql_table.c.content])
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
        table_spec = self.tables[args['TableName']]
        sql_table = table_spec['sql_table']
        key_spec = table_spec['key_spec']
        table_def = table_spec['table_def']

        unsupported_keys = set(args.keys()) - set(['Limit', 'TableName', 'HashKeyValue', 'ScanIndexForward', 'ConsistentRead', 'RangeKeyCondition', 'AttributesToGet', 'ExclusiveStartKey']) 
        if unsupported_keys:
            raise NotImplementedError(unsupported_keys)

        scan_forward = args.get('ScanIndexForward', True)

        expr = sql_table.c.hash_key == utils.parse_value(args['HashKeyValue'])

        if 'RangeKeyCondition' in args:
            if len(key_spec) < 2:
                raise NotImplementedError

            operator = args['RangeKeyCondition']['ComparisonOperator']
            if operator == 'BETWEEN':
                start = utils.parse_value(args['RangeKeyCondition']['AttributeValueList'][0])
                end = utils.parse_value(args['RangeKeyCondition']['AttributeValueList'][1])
                expr = sql.and_(expr, sql_table.c.range_key.between(start, end))

            else:
                range_value = utils.parse_value(args['RangeKeyCondition']['AttributeValueList'][0])
                if operator == 'GT':
                    expr = sql.and_(expr, sql_table.c.range_key > range_value)
                elif operator == 'LT':
                    expr = sql.and_(expr, sql_table.c.range_key < range_value)
                elif operator == 'GE':
                    expr = sql.and_(expr, sql_table.c.range_key >= range_value)
                elif operator == 'LE':
                    expr = sql.and_(expr, sql_table.c.range_key <= range_value)
                elif operator == 'EQ':
                    expr = sql.and_(expr, sql_table.c.range_key == range_value)
                elif operator == 'BEGINS_WITH':
                    expr = sql.and_(expr, sql_table.c.range_key.like('%s%%' % range_value))
                else:
                    raise NotImplementedError

        if 'ExclusiveStartKey' in args:
            range_key = utils.parse_value(args['ExclusiveStartKey']['RangeKeyElement'])
            if scan_forward:
                expr = sql.and_(expr, sql_table.c.range_key > range_key)
            else:
                expr = sql.and_(expr, self.table.c.range_key < range_key)

        q = sql.select([sql_table], expr)

        default_limit = False
        if 'Limit' in args:
            q = q.limit(args['Limit'])
        else:
            default_limit = True
            q = q.limit(DEFAULT_LIMIT + 1)

        if len(key_spec) > 1:
            if scan_forward:
                q = q.order_by(sql_table.c.range_key.asc())
            else:
                q = q.order_by(sql_table.c.range_key.desc())

        out = {'Items': [], 'Count': 0}
        for res in self.engine.execute(q):
            item_data = json.loads(res[sql_table.c.content])
            item = utils.parse_item(item_data)
            if 'AttributesToGet' in args:
                out_item = dict((col_name, item_data[col_name]) for col_name in args['AttributesToGet'])
                out['Items'].append(out_item)
            else:
                out['Items'].append(item_data)
            out['Count'] += 1

            if default_limit and out['Count'] == DEFAULT_LIMIT:
                out['LastEvaluatedKey'] = utils.format_item({'HashKeyElement': res[sql_table.c.hash_key], 'RangeKeyElement': res[sql_table.c.range_key]})
                break

        out['Count'] = len(out['Items'])

        return out

    def make_request(self, command, body=None, callback=None):
        args = json.loads(body)

        capacity = utils.predict_capacity_usage(command, args)

        result = None
        try:
            result = getattr(self, "do_%s" % command.lower())(args)
        except CommandError, e:
            callback(None, error=e.args[0])
            return
        else:
            if result is None:
                result = {}
            if isinstance(capacity, (float, int)):
                result['ConsumedCapacityUnits'] = capacity
            else:
                for table_name, value in capacity.iteritems():
                    result.setdefault('Responses', {}).setdefault(table_name, {})['ConsumedCapacityUnits'] = value

            callback(result, error=None)


class SQLDB(db.BaseDB):
    def __init__(self, engine):
        super(SQLDB, self).__init__()
        self.client = SQLClient(engine)

    def register(self, table, create=False):
        super(SQLDB, self).register(table)
        self.client.register_table(table)


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

