from testify import *
from dynochemy import utils

class SimpleParseTestCase(TestCase):
    def test_string(self):
        value = {'S': 'Rhett'}
        res = utils.parse_value(value)
        assert_equal(res, "Rhett")

    def test_int(self):
        value = {'N': '123'}
        res = utils.parse_value(value)
        assert_equal(res, 123)
        assert_equal(type(res), int)

    def test_float(self):
        value = {'N': '10.5'}
        res = utils.parse_value(value)
        assert_equal(res, 10.5)
        assert_equal(type(res), float)

    def test_multistring(self):
        value = {'SS': ['Rhett', 'Ziggy']}
        res = utils.parse_value(value)
        assert_equal(res, ['Rhett', 'Ziggy'])

class BuildKeyTestCase(TestCase):
    def test_single_string(self):
        key = utils.build_key(('user',), 'Rhett')
        assert_equal(key, {'user': {'S': 'Rhett'}})

    def test_multi_string(self):
        key = utils.build_key(('user', 'id'), ('Rhett', 'foo'))
        assert_equal(key, {'user': {'S': 'Rhett'}, 'id': {'S': 'foo'}})

    def test_multi_number(self):
        key = utils.build_key(('user', 'id'), ('Rhett', 123))
        assert_equal(key, {'user': {'S': 'Rhett'}, 'id': {'N': '123'}})
