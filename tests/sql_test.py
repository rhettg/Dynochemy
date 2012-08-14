import sqlalchemy

from testify import *
from dynochemy import db, Table
from dynochemy import sql
from dynochemy import operation
from dynochemy import errors

class TestTable(Table):
    name = "test"
    hash_key = 'key'


class ReadCapacityTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)
        TestTable.read_capacity = 0.0
        self.db.register(TestTable)

    @setup
    def build_entity(self):
        self.entity = {'key': 'hello', 'count': 0}
        self.db.TestTable.put(self.entity)
        self.db.TestTable.get('hello')

    def test(self):
        try:
            self.db.TestTable.get('hello')
        except errors.ProvisionedThroughputError:
            pass
        else:
            assert False, "Failed to throw exception"


class WriteCapacityTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)
        TestTable.write_capacity = 0.0
        self.db.register(TestTable)

    @setup
    def prime_table(self):
        self.db.TestTable.put({'key': 'prime'})

    def test(self):
        try:
            entity = {'key': 'hello', 'count': 0}
            self.db.TestTable.put(entity)
        except errors.ProvisionedThroughputError:
            pass
        else:
            assert False, "Failed to throw exception"
