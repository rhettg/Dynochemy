import time
import pprint

import sqlalchemy

from testify import *
from dynochemy import db
from dynochemy import operation
from dynochemy import sql
from dynochemy import Table, Index

class TestIndex(Index):
    @classmethod
    def add(self, entity):
        return [operation.UpdateOperation(TestTable, "__totals__", put={'type': 'counter'}, add={'%s_count' % entity['type']: entity['count']})]

    @classmethod
    def remove(self, entity):
        return [operation.UpdateOperation(TestTable, "__totals__", put={'type': 'counter'}, add={'%s_count' % entity['type']: -entity['count']})]


class TestTable(Table):
    name = "test"
    hash_key = 'key'

    indexes = [TestIndex]


class SimpleTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)
        self.db.register(TestTable)


    @setup
    def build_entities(self):
        self.entities = [
            {'key': 'blue', 'type': 'color'},
            {'key': 'orange', 'type': 'color'}, 
            {'key': 'square', 'type': 'shape'}, 
            {'key': 'circle', 'type': 'shape'}, 
            {'key': 'triangle', 'type': 'shape'}, 
        ]

    def test(self):
        updates = []
        for e in self.entities:
            updates += TestTable.put_op(e)

        reduce(operation.reduce_operations, updates).run(self.db)

        totals = self.db.TestTable.get("__totals__")
        assert_equal(totals['all_count'], 5)
        assert_equal(totals['color_count'], 2)
        assert_equal(totals['shape_count'], 3)

        for e in self.db.TestTable.scan()():
            counts[e['type']] += 1

        assert_equal(counts['color'], 2)
        assert_equal(counts['color'], 3)

