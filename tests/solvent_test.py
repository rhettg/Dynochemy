import time
import pprint

import sqlalchemy

from testify import *
from dynochemy import db, Table, Solvent, View
from dynochemy import sql
from dynochemy import operation
from dynochemy import operation

class TestTable(Table):
    name = "test"
    hash_key = 'key'


class SolventTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)
        self.db.register(TestTable)


class SimpleSolventTestCase(SolventTestCase):
    def test(self):
        solvent = Solvent()
        put_op = solvent.put(TestTable, {'key': 'hello', 'value': 10.0})
        result = solvent.run(self.db)
        
        ret, err = result[put_op]
        if err:
            raise err

        assert self.db.TestTable.get('hello')['value'] == 10


class GetSolventTestCase(SolventTestCase):
    @setup
    def build_entity(self):
        self.db.TestTable.put({'key': 'hello', 'value': 25})

    def test(self):
        solvent = Solvent()
        get_op = solvent.get(TestTable, 'hello')
        result = solvent.run(self.db)
        
        ret, err = result[get_op]
        if err:
            raise err

        assert_equal(ret['value'], 25)


class SolventCapacityTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)

    @setup
    def build_new_table(self):
        class LimitedTestTable(Table):
            name = "test"
            hash_key = 'key'

            write_capacity = 1.0
            read_capacity = 1.0
        self.db.register(LimitedTestTable)    
        self.LimitedTestTable = LimitedTestTable

    def test(self):
        solvent = Solvent()
        put_op_1 = solvent.put(self.LimitedTestTable, {'key': 'hello', 'value': 25})
        put_op_2 = solvent.put(self.LimitedTestTable, {'key': 'world', 'value': 100})
        put_op_3 = solvent.put(self.LimitedTestTable, {'key': 'you', 'value': 0})

        result = solvent.run(self.db)
        for op in [put_op_1, put_op_2, put_op_3]:
            _, err = result[op]
            assert not err, err

        solvent = Solvent()
        get_op_1 = solvent.get(self.LimitedTestTable, 'hello')
        get_op_2 = solvent.get(self.LimitedTestTable, 'world')
        get_op_3 = solvent.get(self.LimitedTestTable, 'you')

        result = solvent.run(self.db)
        for op in [get_op_1, get_op_2, get_op_3]:
            entity, err = result[op]
            assert not err, err
            assert entity['key']


class SolventSequenceTestCase(SolventTestCase):
    @setup
    def build_by_two_op(self):
        class ByTwoOperation(operation.UpdateOperation):
            def result(self, r):
                # return an equivalent update operation
                return [operation.UpdateOperation(self.table, self.key, add=self.add)]

        self.op = ByTwoOperation(self.db.TestTable.__class__, 'rhettg', add={'value': 1})

    def test(self):
        solvent = Solvent()
        solvent.add_operation(self.op)
        solvent.run(self.db)

        for res in self.db.TestTable.scan()():
            assert_equal(res['value'], 2)


class SolventViewTestCase(SolventTestCase):
    @setup
    def build_view(self):
        class ViewTable(Table):
            name = "view_table"
            hash_key = 'value'

        self.ViewTable = ViewTable
        self.db.register(ViewTable)

        class TestView(View):
            table = TestTable
            view_table = ViewTable

            def add(self, entity):
                return [operation.UpdateOperation(self.view_table, entity['value'], {'count': 1})]
            def remove(self, entity):
                return [operation.UpdateOperation(self.view_table, entity['value'], {'count': -1})]

        self.TestView = TestView
        self.db.register(TestView)

    def test(self):
        s = Solvent()
        s.put(TestTable, {'key': '1', 'value': 'blue'})
        s.put(TestTable, {'key': '2', 'value': 'green'})
        s.put(TestTable, {'key': '3', 'value': 'blue'})

        s.run(self.db)

        assert_equal(self.db.ViewTable['green']['count'], 1)
        assert_equal(self.db.ViewTable['blue']['count'], 2)

        s = Solvent()
        s.delete(TestTable, '1')
        s.delete(TestTable, '2')
        s.run(self.db)

        assert_equal(self.db.ViewTable['green']['count'], 0)
        assert_equal(self.db.ViewTable['blue']['count'], 1)

