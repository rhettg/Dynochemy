import time

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
        for item in self.items:
            batch.put(item)

