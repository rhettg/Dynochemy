import pprint

from testify import *
import testutil

import dynochemy
from dynochemy import operation

class SimpleTest(TestCase):
    @setup
    def build_view(self):
        class TestView(dynochemy.View):
            def add(self, entity):
                return [turtle.Turtle()]
        self.TestView = TestView

    @setup
    def build_db(self):
        class TestTable(dynochemy.Table):
            hash_key = 'user'

        self.TestTable = TestTable

        self.db = turtle.Turtle()

    def test(self):
        v = self.TestView(self.db)

        put = operation.PutOperation(self.TestTable, {'user': 'test'})

        prev_ops, current_ops = v.operations_for_operation(put)
        assert not prev_ops
        assert current_ops

class GetAndRemoveTestCase(testutil.DBTestCase):
    @setup
    def build_view(self):
        class TestView(dynochemy.View):
            def remove(self, entity):
                # We'll just put the entity back, with a different value
                return [operation.PutOperation(testutil.TestTable, {'key': entity['key'], 'value': None})]

        self.TestView = TestView

    @setup
    def put_entity(self):
        self.db.TestTable.put({'key': 'rhettg', 'value': 10})

    def test(self):
        v = self.TestView(self.db)

        del_op = operation.DeleteOperation(testutil.TestTable, 'rhettg')

        prev_ops, current_ops = v.operations_for_operation(del_op)

        assert prev_ops
        assert not current_ops

        res = prev_ops[0].run(self.db)

        assert res.next_ops
        res.next_ops[0].run(self.db)

        assert 'value' not in self.db.TestTable['rhettg']


