from testify import *

import sqlalchemy
import dynochemy
from dynochemy import db
from dynochemy import sql

class TestTable(dynochemy.Table):
    name = "test"
    hash_key = 'key'


class TestDB(db.BaseDB):
    def __init__(self):
        super(TestDB, self).__init__()

        self.allow_sync = True
        self.ioloop = None
        self.client = turtle.Turtle()


class DBTestCase(TestCase):
    @setup
    def build_db(self):
        engine = sqlalchemy.create_engine("sqlite://")
        self.db = sql.SQLDB(engine)
        self.db.register(TestTable)
