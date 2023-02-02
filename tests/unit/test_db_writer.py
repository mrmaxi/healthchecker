from datetime import datetime
from unittest import mock

from pytest import mark, param

from db_writer import parse_message


TEST_RECORD1 = {
    'id': 'd4a549fd-0907-4d64-92d6-bcc492d4f7ce',
    'check_name': 'test1',
    'dt': datetime(2023, 1, 1, 10, 0, 0),
    'health': True,
    'status': '200',
    'duration': 0.1,
    'length': 300,
    'sample': '170'
}

TEST_RECORD2 = {
    'id': '3598c988-4cb3-4d34-a01a-237feab8228b',
    'check_name': 'test2',
    'dt': datetime(2023, 1, 1, 11, 0, 0),
    'health': False,
    'status': '500',
    'duration': 0.5,
    'length': 100,
    'sample': None
}

TEST_INCOMPLETE_RECORD = {
    'id': '3598c988-4cb3-4d34-a01a-237feab8228b',
    'check_name': 'incomplete',
    'dt': datetime(2023, 1, 1, 13, 0, 0),
    'health': False,
}

TEST_INCOMPLETE_RECORD_PARSED = {
    'id': '3598c988-4cb3-4d34-a01a-237feab8228b',
    'check_name': 'incomplete',
    'dt': datetime(2023, 1, 1, 13, 0, 0),
    'health': False,
    'status': None,
    'duration': None,
    'length': None,
    'sample': None
}

TEST_BAD_RECORD = {
    'a': 123
}


@mark.parametrize("record, expected", [
    param(TEST_RECORD1, TEST_RECORD1),
    param(TEST_RECORD2, TEST_RECORD2),
    param(TEST_INCOMPLETE_RECORD, TEST_INCOMPLETE_RECORD_PARSED),
    param(TEST_BAD_RECORD, None),
])
def test_parse_message(record, expected):
    message = mock.Mock()
    message.partition = 0
    message.offset = 15
    message.value = record
    actual = parse_message(message)
    assert actual == expected
