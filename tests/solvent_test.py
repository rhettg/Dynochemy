import time
import pprint

import sqlalchemy

from testify import *
from dynochemy import db, Table, Solvent
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

class SolventCapacityTestCase(SolventTestCase):
    __test__ = False
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

        solvent = Solvent()
        get_op_1 = solvent.get(self.LimitedTestTable, 'hello')
        get_op_2 = solvent.get(self.LimitedTestTable, 'world')
        get_op_3 = solvent.get(self.LimitedTestTable, 'you')

        result = solvent.run(self.db)



