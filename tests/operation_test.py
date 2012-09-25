import time
import pprint

import sqlalchemy

from testify import *
from dynochemy import db, Table
from dynochemy import sql
from dynochemy import operation
from dynochemy import constants

class TestTable(Table):
    name = "test"
    hash_key = 'key'

class FullTestTable(Table):
    name = "full_test"
    hash_key = 'key'
    range_key = 'range_key'

class TestDB(db.BaseDB):
    def __init__(self):
        super(TestDB, self).__init__()

        self.allow_sync = True
        self.ioloop = None
        self.client = turtle.Turtle()

class OperationTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)
        self.db.register(TestTable)
        self.db.register(FullTestTable)


class SimplePutTestCase(OperationTestCase):
    @setup
    def build_operation(self):
        self.entity = {'key': 'hello', 'count': 0}
        self.op = operation.PutOperation(TestTable, self.entity)
        self.results = operation.OperationResult(self.db)

    def test(self):
        self.op.run(self.results)
        assert self.results[self.op]

        entity = self.db.TestTable.get("hello")
        assert entity
        assert entity['count'] == 0


class SimpleDeleteTestCase(OperationTestCase):
    @setup
    def build_entity(self):
        self.entity = {'key': 'hello', 'count': 0}
        self.db.TestTable.put(self.entity)

    @setup
    def build_operation(self):
        self.op = operation.DeleteOperation(TestTable, 'hello')
        self.results = operation.OperationResult(self.db)

    def test(self):
        self.op.run(self.results)

        assert self.results[self.op]

        try:
            entity = self.db.TestTable.get("hello")
        except KeyError:
            pass
        else:
            assert not entity, entity


class BatchWriteTestCase(OperationTestCase):
    @setup
    def build_entity(self):
        self.entity = {'key': 'hello', 'count': 0}
        self.db.TestTable.put(self.entity)

        self.new_entity = {'key': 'world', 'count': 1}

    @setup
    def build_operation(self):
        put_op = operation.PutOperation(TestTable, self.new_entity)
        delete_op = operation.DeleteOperation(TestTable, 'hello')
        self.results = operation.OperationResult(self.db)

        self.sub_ops = [put_op, delete_op]
        self.batch_op = operation.BatchWriteOperation()
        for op in self.sub_ops:
            self.batch_op.add(op)

    def test(self):
        results = self.batch_op.run(self.results)

        for op in self.sub_ops:
            assert self.results[op]

        try:
            entity = self.db.TestTable.get("hello")
        except KeyError:
            pass
        else:
            assert not entity, entity

        entity = self.db.TestTable.get("world")
        assert entity
        assert_equal(entity['count'], 1)

        assert_equal(self.results.write_capacity[self.db.TestTable.name], 2.0)


class UpdateTestCase(OperationTestCase):
    @setup
    def build_entity(self):
        self.entity = {'key': 'hello', 'count': 1}
        self.db.TestTable.put(self.entity)

    @setup
    def build_operation(self):
        self.op = operation.UpdateOperation(TestTable, self.entity['key'], add={'count': 1})
        self.results = operation.OperationResult(self.db)

    def test(self):
        result = self.op.run(self.results)

        entity = self.db.TestTable.get("hello")
        assert_equal(entity['count'], 2)
        assert_equal(self.results[self.op][0]['count'], 2)


class CombineUpdateTestCase(OperationTestCase):
    @setup
    def build_op(self):
        self.op = operation.UpdateOperation(TestTable, 'hello', put={'my_name': 'slim shady'}, add={'count': 1})
        self.results = operation.OperationResult(self.db)

    def test(self):
        op = self.op.combine_updates(self.op)
        op = op.combine_updates(self.op)

        result = op.run(self.results)

        entity = self.db.TestTable.get('hello')
        assert_equal(entity['my_name'], 'slim shady')
        assert_equal(entity['count'], 3)

        val, err = self.results[op]
        assert not err


class UpdateCombineTestCase(OperationTestCase):
    def test_add_count(self):
        op_1 = operation.UpdateOperation(TestTable, 'hello', put={'my_name': 'slim shady'}, add={'count': 1})
        op_2 = operation.UpdateOperation(TestTable, 'hello', put={'my_name': 'slim shady'}, add={'count': 4})

        full_op = op_1.combine_updates(op_2)

        assert_equal(full_op.put['my_name'], 'slim shady')
        assert_equal(full_op.add['count'], 5)

    def test_add_str(self):
        op_1 = operation.UpdateOperation(TestTable, 'hello', delete={'my_name': None}, add={'count': "hello "})
        op_2 = operation.UpdateOperation(TestTable, 'hello', delete={'my_name': None}, add={'count': "world"})

        full_op = op_1.combine_updates(op_2)

        assert_equal(full_op.delete['my_name'], None)
        assert_equal(full_op.add['count'], "hello world")


class GetTestCase(OperationTestCase):
    @setup
    def build_entity(self):
        self.entity = {'key': 'hello', 'count': 1}
        self.db.TestTable.put(self.entity)

    @setup
    def build_operation(self):
        self.op = operation.GetOperation(TestTable, self.entity['key'])
        self.result = operation.OperationResult(self.db)

    def test(self):
        self.op.run(self.result)

        entity, err = self.result[self.op]
        assert not err

        assert_equal(entity['count'], 1)


class MultiGetTestCase(OperationTestCase):
    @setup
    def build_entity(self):
        entity = {'key': 'hello', 'count': 1}
        self.db.TestTable.put(entity)

        entity = {'key': 'world', 'count': 10}
        self.db.TestTable.put(entity)

    @setup
    def build_ops(self):
        self.result = operation.OperationResult(self.db)
        self.ops = [
            operation.GetOperation(TestTable, "hello"),
            operation.GetOperation(TestTable, "world"),
        ]

        self.batch_op = operation.BatchReadOperation(self.ops)

    def test(self):
        result = self.batch_op.run(self.result)

        op_1, op_2 = self.ops
        entity, err = self.result[op_1]
        if err:
            raise err
        assert_equal(entity['count'], 1)

        entity, err = self.result[op_2]
        assert not err
        assert_equal(entity['count'], 10)


class MultiBatchReadTestCase(OperationTestCase):
    @setup
    def max_items(self):
        self.orig_max_items = constants.MAX_BATCH_READ_ITEMS
        constants.MAX_BATCH_READ_ITEMS = 2

    @teardown
    def un_max_items(self):
        constants.MAX_BATCH_READ_ITEMS = self.orig_max_items

    @setup
    def build_entities(self):
        self.keys = []
        for ndx in range(4):
            key = 'entity_%d' % ndx
            self.keys.append(key)
            entity = {'key': key, 'value': ndx}
            self.db.TestTable.put(entity)

    @setup
    def build_ops(self):
        self.result = operation.OperationResult(self.db)
        self.ops = [operation.GetOperation(TestTable, key) for key in self.keys]

        self.batch_op = operation.BatchReadOperation(self.ops)

    def test(self):
        results = self.batch_op.run(self.result)

        assert self.result.next_ops

        next_batch = self.result.next_ops[0]
        next_batch.run(self.result)

        for key, op in zip(self.keys, self.ops):
            val, err = self.result[op]
            assert not err
            assert_equal(val['key'], key)

        assert_equal(self.result.read_capacity[self.db.TestTable.name], 4.0)


class QueryOperationTestCase(OperationTestCase):
    @setup
    def build_entities(self):
        self.keys = []
        for ndx in range(4):
            entity = {'key': 'my_key', 'range_key': ndx}
            self.db.FullTestTable.put(entity)

    @setup
    def change_query_limit(self):
        self._old_limit = sql.DEFAULT_LIMIT
        sql.DEFAULT_LIMIT = 2

    @teardown
    def restore_query_limit(self):
        sql.DEFAULT_LIMIT = self._old_limit

    @setup
    def create_op(self):
        self.op = operation.QueryOperation(FullTestTable, 'my_key')

        self.op.range(0, 2)
        self.op.limit(20)
        self.results = operation.OperationResult(self.db)

    def test(self):

        self.op.run(self.results)

        query_result, err = self.results[self.op]
        if err:
            raise err

        # We should only have 2, because that's our DEFAULT_LIMIT
        entities = list(query_result)
        assert_equal(len(entities), 2)

        assert self.results.next_ops
        
        next_op = self.results.next_ops.pop()
        next_op.run(self.results)

        query_result, err = self.results[self.op]
        if err:
            raise err

        entities = list(query_result)
        assert_equal(len(entities), 3)

