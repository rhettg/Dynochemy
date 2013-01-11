import time
import pprint

from testify import *
from dynochemy import db, Table, DynamoDBError

class TestDB(db.BaseDB):
    def __init__(self):
        super(TestDB, self).__init__()

        self.allow_sync = True
        self.ioloop = None
        self.client = turtle.Turtle()


class SimpleTest(TestCase):
    def test(self):
        self.db = TestDB()

        class TestTable(Table):
            name = "test"
            hash_key = 'user'
            range_key = 'time'

        self.db.register(TestTable)
        

class UpdateTest(TestCase):
    @setup
    def create_client(self):
        self.db = TestDB()

        class TestTable(Table):
            name = "test"
            hash_key = 'user'
            range_key = 'day'

        self.db.register(TestTable)

        def make_request(*args, **kwargs):
            kwargs['callback']({}, error=None)

        self.db.client.make_request = make_request

    def test_add(self):
        self.db.TestTable.update(("rhett", "2012-01-01"), add={"counter": 1})

    def test_put(self):
        self.db.TestTable.update(("rhett", "2012-01-01"), put={"scalar": 1})

    def test_delete(self):
        self.db.TestTable.update(("rhett", "2012-01-01"), delete={"scalar": 1})


class SimpleBatchTest(TestCase):
    @setup
    def build_db(self):
        self.db = TestDB()

        class TestTable(Table):
            name = "test"
            hash_key = 'user'
            range_key = 'time'

        self.db.register(TestTable)

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
            item_dfs.append(batch.TestTable.put(item))

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
            item_dfs.append(batch.TestTable.put(item))

        d = batch.defer()
        callback = self.db.client.make_request.calls[0][1]['callback']

        class CustomError(DynamoDBError):
            data = '{"__type": "UnknownError", "message": "this is an error"}'
        callback({}, error=CustomError())

        assert d.done
        for df in item_dfs:
            assert df.done
            assert df.error

        assert_equal(len(batch.errors), 1)


class BatchSyncFailTest(TestCase):
    @setup
    def build_error(self):
        class CustomError(DynamoDBError):
            data = '{"__type": "UnknownError", "message": "this is an error"}'

        self.error = CustomError()

    @setup
    def build_db(self):
        self.db = TestDB()

        def make_request(*args, **kwargs):
            kwargs['callback'](None, self.error)

        self.db.client.make_request = make_request

        class TestTable(Table):
            name = "test"
            hash_key = 'user'
            range_key = 'time'

        self.db.register(TestTable)

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
            item_dfs.append(batch.TestTable.put(item))

        try:
            batch()
        except DynamoDBError:
            pass
        else:
            assert False, "Should have failed"
            
        assert_equal(len(batch.errors), 1)


