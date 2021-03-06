# -*- coding: utf-8 -*-
"""
SQLite emulation of DynamoDB
This is useful for local development and testing.

:copyright: (c) 2012 by Rhett Garber.
:license: ISC, see LICENSE for more details.
"""
import json
import collections
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
        self.connection = None

        self.tables = {}
        self.metadata = sqlalchemy.MetaData(bind=engine)

        # During an operation, we keep track of how much capacity is being used per table
        self.reset_op_capacity()

    def reset_op_capacity(self):
        self.table_read_op_capacity = collections.defaultdict(int)
        self.table_write_op_capacity = collections.defaultdict(int)

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

        read_counter = utils.ResourceCounter()
        if table_cls.read_capacity is not None:
            read_counter.set_limit(1, table_cls.read_capacity)
            read_counter.set_limit(5, table_cls.read_capacity * 0.80)

        write_counter = utils.ResourceCounter()
        if table_cls.write_capacity is not None:
            write_counter.set_limit(1, table_cls.write_capacity)
            write_counter.set_limit(5, table_cls.write_capacity * 0.80)

        self.tables[table_cls.name] = {
            'sql_table': sql_table,
            'table_def': table_cls,
            'key_spec': key_spec,
            'read_counter': read_counter,
            'write_counter': write_counter,
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
            capacity_size = (len(res[sql_table.c.content]) / 1024) + 1
            self.table_read_op_capacity[table_name] += capacity_size
            self.tables[table_name]['read_counter'].record(capacity_size)
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

        item_json = json.dumps(item_data)

        capacity_size = (len(item_json) / 1024) + 1
        self.table_write_op_capacity[table_name] += capacity_size
        self.tables[table_name]['write_counter'].record(capacity_size)

        if table_def.range_key:
            range_key = item[key_spec[1]]
            del_q = sql_table.delete().where(sql.and_(sql_table.c.hash_key==hash_key, sql_table.c.range_key==range_key))
            ins = sql_table.insert().values(hash_key=hash_key, range_key=range_key, content=item_json)
        else:
            del_q = sql_table.delete().where(sql_table.c.hash_key==hash_key)
            ins = sql_table.insert().values(hash_key=hash_key, content=item_json)

        cnxn = self.connection or self.engine

        try:
            cnxn.execute(ins)
        except sqlalchemy.exc.IntegrityError:
            # Try again
            txn = None
            if cnxn == self.engine:
                cnxn = self.engine.connect()
                txn = cnxn.begin()

            cnxn.execute(del_q)
            cnxn.execute(ins)

            if txn:
                txn.commit()
        
        return capacity_size

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

        # This one is kinda weird, we're basically gonna transfer whatever we
        # racked up in reads to the write column.
        op_capacity = self.table_read_op_capacity[args['TableName']]
        self.table_read_op_capacity[args['TableName']] = 0
        self.tables[args['TableName']]['read_counter'].record(op_capacity * -1)

        self.table_write_op_capacity[args['TableName']] += op_capacity
        self.tables[args['TableName']]['write_counter'].record(op_capacity)

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

        # Update is one of our few transactionally important operations.  By
        # setting self.connection, our callees should use that rather than the
        # connectionless self.engine method, allowing us to control the
        # transaction directly.
        self.connection = self.engine.connect()
        txn = self.connection.begin()

        q = sql.select([sql_table], expr)
        res = list(self.connection.execute(q))
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
                    if isinstance(real_item[attribute], (int,float,list)):
                        real_item[attribute] += utils.parse_value(value_update['Value'])
                    elif isinstance(real_item[attribute], list):
                        if hasattr(value_update['Value'], '__iter__'):
                            real_item[attribute] += [utils.parse_value(v) for v in value_update['Value']]
                        else:
                            real_item[attribute].append(utils.parse_value(value_update['Value']))
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

        txn.commit()
        self.connection = None

        if args.get('ReturnValues', 'NONE') == 'NONE':
            return {}
        elif args['ReturnValues'] == 'ALL_NEW':
            return {'Attributes': utils.format_item(real_item)}
        elif args['ReturnValues'] == 'ALL_OLD':
            return {'Attributes': item}

    def do_batchwriteitem(self, args):
        out = {}
        for table_name, requests in args['RequestItems'].iteritems():
            for request in requests:
                req_type = request.keys()[0]
                args = request[req_type]
                args['TableName'] = table_name

                if req_type == "PutRequest":
                    if not self.tables[table_name]['write_counter'].check():
                        out.setdefault('UnprocessedItems', {})
                        out['UnprocessedItems'].setdefault(table_name, [])
                        out['UnprocessedItems'][table_name].append(request)
                    else:
                        self._put_item(table_name, args['Item'])

                elif req_type == "DeleteRequest":
                    # TODO: write capacity checks
                    self.do_deleteitem(args)
                else:
                    raise NotImplementedError

        return out

    def do_batchgetitem(self, args):
        out = {'Responses': {}}
        for table_name, requests in args['RequestItems'].iteritems():
            out['Responses'].setdefault(table_name, {'Items': []})
            for key in requests['Keys']:
                # We'll go through and get each item as long as our capcity can handle it.
                if not self.tables[table_name]['read_counter'].check():
                    out.setdefault('UnprocessedKeys', {})
                    out['UnprocessedKeys'].setdefault(table_name, {'Keys': []})
                    out['UnprocessedKeys'][table_name]['Keys'].append(key)
                else:
                    # TODO: This would probably be nice as a better multi-key query
                    item = self._get_item(table_name, key)
                    if item:
                        out['Responses'][table_name]['Items'].append(item)

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

        capacity_size = 0
        out = {'Items': []}
        for res in self.engine.execute(q):
            capacity_size += (len(res[sql_table.c.content]) / 1024) + 1
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

        self.table_read_op_capacity[args['TableName']] += capacity_size
        self.tables[args['TableName']]['read_counter'].record(capacity_size)

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

        if 'Limit' in args:
            limit = min(DEFAULT_LIMIT, args['Limit'])
        else:
            limit = DEFAULT_LIMIT

        q = q.limit(limit + 1)

        if len(key_spec) > 1:
            if scan_forward:
                q = q.order_by(sql_table.c.range_key.asc())
            else:
                q = q.order_by(sql_table.c.range_key.desc())

        capacity_size = 0
        out = {'Items': [], 'Count': 0}
        for res in self.engine.execute(q):
            if out['Count'] == limit:
                break

            capacity_size += (len(res[sql_table.c.content]) / 1024) + 1

            item_data = json.loads(res[sql_table.c.content])
            item = utils.parse_item(item_data)
            if 'AttributesToGet' in args:
                out_item = dict((col_name, item_data[col_name]) for col_name in args['AttributesToGet'])
                out['Items'].append(out_item)
            else:
                out['Items'].append(item_data)

            out['Count'] += 1
            if len(key_spec) > 1:
                out['LastEvaluatedKey'] = utils.format_item({'HashKeyElement': res[sql_table.c.hash_key], 'RangeKeyElement': res[sql_table.c.range_key]})
        else:
            # If we didn't break out of our loop, that means we're on the last page of our result set and should have a key

            # The docs say the last evaulated key should be 'null', but I'm suspicious that isn't the case.
            if 'LastEvaluatedKey' in out:
                #out['LastEvaluatedKey'] = None
                del out['LastEvaluatedKey']
            

        self.table_read_op_capacity[args['TableName']] += capacity_size
        self.tables[args['TableName']]['read_counter'].record(capacity_size)

        out['Count'] = len(out['Items'])

        return out

    def make_request(self, command, body=None, callback=None):
        self.reset_op_capacity() 

        args = json.loads(body)
        # Record and check our provisioning limits
        if 'TableName' in args:
            if command in ('GetItem', 'BatchGetItem') and not self.tables[args['TableName']]['read_counter'].check():
                return callback({}, error=errors.ProvisionedThroughputError())

            if command in ('PutItem', 'BatchWriteItem', 'DeleteItem', 'UpdateItem') and not self.tables[args['TableName']]['write_counter'].check():
                return callback({}, error=errors.ProvisionedThroughputError())

        result = None
        try:
            result = getattr(self, "do_%s" % command.lower())(args)
        except CommandError, e:
            callback({}, error=e.args[0])
            return
        else:
            if result is None:
                result = {}

            # Record and check our provisioning limits
            if 'TableName' in args:
                if self.table_read_op_capacity:
                    for _, capacity in self.table_read_op_capacity.iteritems():
                        result['ConsumedCapacityUnits'] = float(capacity)

                elif self.table_write_op_capacity:
                    for _, capacity in self.table_write_op_capacity.iteritems():
                        result['ConsumedCapacityUnits'] = float(capacity)

            else:
                for table_name, capacity in self.table_read_op_capacity.iteritems():
                    result.setdefault('Responses', {}).setdefault(table_name, {})['ConsumedCapacityUnits'] = float(capacity)

                for table_name, capacity in self.table_write_op_capacity.iteritems():
                    result.setdefault('Responses', {}).setdefault(table_name, {})['ConsumedCapacityUnits'] = float(capacity)

            callback(result, error=None)


class SQLDB(db.BaseDB):
    def __init__(self, engine):
        super(SQLDB, self).__init__()
        self.client = SQLClient(engine)

    def register(self, table, create=False):
        super(SQLDB, self).register(table)
        if issubclass(table, db.Table):
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

