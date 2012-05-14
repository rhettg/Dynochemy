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


class FormatKeyTestCase(TestCase):
    def test_single_string(self):
        key = utils.format_key(('user',), 'Rhett')
        assert_equal(key, {'user': {'S': 'Rhett'}})

    def test_multi_string(self):
        key = utils.format_key(('user', 'id'), ('Rhett', 'foo'))
        assert_equal(key, {'user': {'S': 'Rhett'}, 'id': {'S': 'foo'}})

    def test_multi_number(self):
        key = utils.format_key(('user', 'id'), ('Rhett', 123))
        assert_equal(key, {'user': {'S': 'Rhett'}, 'id': {'N': '123'}})

    def test_multi_number_float(self):
        key = utils.format_key(('user', 'id'), ('Rhett', 123.123))
        assert_equal(key, {'user': {'S': 'Rhett'}, 'id': {'N': '123.123'}})


class FormatItemTestCase(TestCase):
    def test(self):
        item = {'user': 'Rhett', 'value': 123.123, 'empty': ''}
        fmt_item = utils.format_item(item)

        assert_equal(fmt_item['user'], {'S': 'Rhett'})
        assert_equal(fmt_item['value'], {'N': '123.123'})
        assert 'empty' not in fmt_item

class ParseItemTestCase(TestCase):
    def test(self):
        item = utils.parse_item({'id': {'S': 'Rhett'}, 'value': {'N': '10.125'}})
        assert_equal(item['id'], 'Rhett')
        assert_equal(item['value'], 10.125)
