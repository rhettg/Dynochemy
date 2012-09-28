import pprint

from testify import *
import testutil

import dynochemy
from dynochemy import operation

class SimpleTest(TestCase):
    @setup
    def build_view(self):
        class TestView(dynochemy.View):
            @classmethod
            def add(cls, op, result):
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

        current_ops = v.operations_for_operation(put, turtle.Turtle())
        assert current_ops



