from testify import *
from dynochemy import db

class SimpleTest(TestCase):
    def test(self):
        self.db = db.DB('RhettTest', ('user', 'time'), '123', '456')
