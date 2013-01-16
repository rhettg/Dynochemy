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


class FullTestTable(Table):
    name = "full_test"
    hash_key = 'key'
    range_key = 'range_key'


class SolventTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)
        self.db.register(TestTable)
        self.db.register(FullTestTable)


class SimpleSolventTestCase(SolventTestCase):
    def test(self):
        solvent = Solvent()
        put_op = solvent.put(TestTable, {'key': 'hello', 'value': 10.0})
        result = solvent.run(self.db)
        
        ret = result[put_op]

        assert self.db.TestTable.get('hello')['value'] == 10


class GetSolventTestCase(SolventTestCase):
    @setup
    def build_entity(self):
        self.db.TestTable.put({'key': 'hello', 'value': 25})

    def test(self):
        solvent = Solvent()
        get_op = solvent.get(TestTable, 'hello')
        result = solvent.run(self.db)
        
        ret = result[get_op]

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
            result[op]

        solvent = Solvent()
        get_op_1 = solvent.get(self.LimitedTestTable, 'hello')
        get_op_2 = solvent.get(self.LimitedTestTable, 'world')
        get_op_3 = solvent.get(self.LimitedTestTable, 'you')

        result = solvent.run(self.db)
        for op in [get_op_1, get_op_2, get_op_3]:
            entity = result[op]
            assert entity['key']


class SolventSequenceTestCase(SolventTestCase):
    @setup
    def build_by_two_op(self):
        class ByTwoOperation(operation.UpdateOperation):
            def have_result(self, op_results, op_cb):
                super(ByTwoOperation, self).have_result(op_results, op_cb)

                # return an equivalent update operation
                op_results.next_ops.append(operation.UpdateOperation(self.table, self.key, add=self.add))

        self.op = ByTwoOperation(self.db.TestTable.__class__, 'rhettg', add={'value': 1})

    def test(self):
        solvent = Solvent()
        solvent.add_operation(self.op)
        solvent.run(self.db)

        for res in self.db.TestTable.scan()():
            assert_equal(res['value'], 2)


class SolventViewViewTestCase(SolventTestCase):
    """Verify that our view created operations go through views also"""
    @setup
    def build_views(self):
        class TestView(View):
            table = TestTable
            view_table = TestTable

            @classmethod
            def add(cls, op, result):
                entity = op.entity
                if entity['key'] == 'A':
                    return [operation.PutOperation(cls.view_table, {'key': 'B'})]
                if entity['key'] == 'B':
                    return [operation.PutOperation(cls.view_table, {'key': 'C'})]
                
                return []

        self.TestView = TestView
        self.db.register(TestView)

    def test(self):
        solvent = Solvent()
        solvent.put(TestTable, {'key': 'A'})
        solvent.run(self.db)

        ndx = 0
        for res in self.db.TestTable.scan()():
            ndx += 1
            #pprint.pprint(res)

        assert_equal(ndx, 3)


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

            @classmethod
            def add(cls, op, result):
                entity = op.entity
                return [operation.UpdateOperation(cls.view_table, entity['value'], {'count': 1})]

            @classmethod
            def remove(cls, op, result):
                return [operation.UpdateOperation(cls.view_table, result['value'], {'count': -1})]

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


class SolventQueryTestCase(SolventTestCase):
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

    def test(self):
        s = Solvent()
        q_op = s.query(FullTestTable, 'my_key')

        q_op.range(0, 2)
        q_op.limit(20)

        result = s.run(self.db)

        query_result = result[q_op]

        entities = list(query_result)
        assert_equal(len(entities), 3)


