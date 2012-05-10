import time
import pprint

from testify import *
from dynochemy import db

class TestDB(db.BaseDB):
    def __init__(self, name, key_spec):
        super(TestDB, self).__init__(name, key_spec)

        self.allow_sync = True
        self.ioloop = None
        self.client = turtle.Turtle()

class SimpleTest(TestCase):
    def test(self):
        self.db = TestDB('RhettTest', ('user', 'time'))

class SimpleBatchTest(TestCase):
    @setup
    def build_db(self):
        self.db = TestDB('RhettTest', ('user', 'time'))

    @setup
    def build_items(self):
        self.items = [
            {'user': 'rhett', 'time': time.time()},
            {'user': 'bryan', 'time': time.time()},
            {'user': 'neil', 'time': time.time()},
        ]

    def test(self):
        batch = self.db.batch_write()
        item_dfs = []
        for item in self.items:
            item_dfs.append(batch.put(item))

        d = batch.defer()
        assert_equal(len(self.db.client.make_request.calls), 1)

        callback = self.db.client.make_request.calls[0][1]['callback']

        callback({})

        assert d.done
        for df in item_dfs:
            assert df.done
