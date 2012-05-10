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

    def test_good(self):
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

    def test_fail(self):
        batch = self.db.batch_write()
        item_dfs = []
        for item in self.items:
            item_dfs.append(batch.put(item))

        d = batch.defer()
        callback = self.db.client.make_request.calls[0][1]['callback']

        callback({}, error="This is a failure")

        assert d.done
        for df in item_dfs:
            assert df.done
            assert df.result[1]

        assert_equal(len(batch.errors), 1)


class MultipleBatchesTest(TestCase):
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
        batch.MAX_ITEMS = 2

        item_dfs = []
        for item in self.items:
            item_dfs.append(batch.put(item))

        d = batch.defer()
        assert_equal(len(self.db.client.make_request.calls), 2)

        callback = self.db.client.make_request.calls[0][1]['callback']

        callback({})

        assert not d.done
        # some should be done, some should not be done
        assert any(d.done for d in item_dfs)
        assert any(not d.done for d in item_dfs)

