import time
import pprint

import sqlalchemy

from testify import *
from dynochemy import db, Table
from dynochemy import sql
from dynochemy import operation

class TestTable(Table):
    name = "test"
    hash_key = 'key'

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


class SimplePutTestCase(OperationTestCase):
    @setup
    def build_operation(self):
        self.entity = {'key': 'hello', 'count': 0}
        self.op = operation.PutOperation(TestTable, self.entity)

    def test(self):
        self.op.run(self.db)

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

    def test(self):
        self.op.run(self.db)

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
        self.batch_op = operation.BatchWriteOperation()
        self.batch_op.add(put_op)
        self.batch_op.add(delete_op)

    def test(self):
        self.batch_op.run(self.db)

        try:
            entity = self.db.TestTable.get("hello")
        except KeyError:
            pass
        else:
            assert not entity, entity

        entity = self.db.TestTable.get("world")
        assert entity
        assert_equal(entity['count'], 1)


class UpdateTestCase(OperationTestCase):
    @setup
    def build_entity(self):
        self.entity = {'key': 'hello', 'count': 1}
        self.db.TestTable.put(self.entity)

    @setup
    def build_operation(self):
        self.op = operation.UpdateOperation(TestTable, self.entity['key'], add={'count': 1})

    def test(self):
        result, err = self.op.run(self.db)
        assert not err

        entity = self.db.TestTable.get("hello")
        assert_equal(entity['count'], 2)
        assert_equal(result[self.op][0]['count'], 2)


class ReduceSimpleTestCase(OperationTestCase):
    def test(self):
        op_1 = operation.PutOperation(TestTable, {'key': 'hello', 'count': 0})
        op_2 = operation.PutOperation(TestTable, {'key': 'world', 'count': 1})
        op_3 = operation.PutOperation(TestTable, {'key': 'you', 'count': 2})

        full_op = reduce(operation.reduce_operations, [op_1, op_2, op_3])
        assert isinstance(full_op, operation.BatchWriteOperation)
        result, err = full_op.run(self.db)
        if err:
            raise err

        entity_1 = self.db.TestTable.get('hello')
        assert_equal(entity_1['count'], 0)

        entity_2 = self.db.TestTable.get('world')
        assert_equal(entity_2['count'], 1)

        entity_3 = self.db.TestTable.get('you')
        assert_equal(entity_3['count'], 2)

        # Now check our result object
        val, err = result[op_1]
        assert not err

        val, err = result[op_2]
        assert not err

        val, err = result[op_3]
        assert not err


class ReduceUpdateTestCase(OperationTestCase):
    def test(self):
        op = operation.UpdateOperation(TestTable, 'hello', put={'my_name': 'slim shady'}, add={'count': 1})
        full_op = reduce(operation.reduce_operations, [op, op, op])

        result, err = full_op.run(self.db)
        assert not err

        entity = self.db.TestTable.get('hello')
        assert_equal(entity['my_name'], 'slim shady')
        assert_equal(entity['count'], 3)

        val, err = result[op]
        assert not err


class DoubleSetTestCase(OperationTestCase):
    def test(self):
        op = operation.UpdateOperation(TestTable, 'hello', put={'my_name': 'slim shady'}, add={'count': 1})
        op_set_1 = reduce(operation.reduce_operations, [op, op])

        op_set_2 = reduce(operation.reduce_operations, [op, op, op])

        full_op = reduce(operation.reduce_operations, [op_set_1, op_set_2])

        full_op.run(self.db)

        entity = self.db.TestTable.get('hello')
        assert_equal(entity['my_name'], 'slim shady')
        assert_equal(entity['count'], 5)

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

    def test_in_operation_set(self):
        op_1 = operation.UpdateOperation(TestTable, 'hello', put={'my_name': 'slim shady'}, add={'count': 1})
        op_2 = operation.UpdateOperation(TestTable, 'hello', put={'my_name': 'slim shady'}, add={'count': 2})

        full_op = reduce(operation.reduce_operations, [op_1, op_2, op_1, op_2])
        assert isinstance(full_op, operation.OperationSet)
        assert_equal(len(full_op.update_ops), 1)



