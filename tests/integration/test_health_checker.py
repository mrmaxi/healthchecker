from datetime import timedelta
from pytest import mark, param, fixture
from unittest import mock

import broker
from broker import create_topic_if_not_exists
from health_checker import start_checking


from .conftest import clear_queue, BROKER_WAIT_TIMEOUT_MS
from ..unit.test_health_checker import set_mock_request

# define settings default values
HEALTH_CHECKER_TIMEOUT = 0.5

# override settings by local values for integration tests
try:
    from .local_settings import *   # noqa: F401, F403
except ImportError:
    pass


TEST_SITES = {
    "test": {
        "url": "http://localhost",
        "minutes": 10
    }
}


@fixture(autouse=True, scope='module')
def my_setup_module(temp_consumer, temp_admin):
    create_topic_if_not_exists(admin=temp_admin, topic_name=broker.TOPIC_NAME)
    clear_queue(temp_consumer)


def convert_message(message: dict) -> dict:
    if message:
        return {k: v for k, v in message.items() if k not in ['dt', 'id', 'duration']}


@mark.parametrize("sites, status, text, duration, expected", [
    param(TEST_SITES, 200, 'aaa', timedelta(seconds=0.01), [
        {'check_name': 'test', 'health': True, 'length': 3, 'sample': None, 'status': 200}]),
])
@mock.patch('aiohttp.request')
def test_start_checking(mock_request, temp_producer, temp_consumer, sites: dict, status, text, duration, expected):
    set_mock_request(mock_request, status, text=text)

    start_checking(temp_producer, sites, timeout=HEALTH_CHECKER_TIMEOUT)

    batches = temp_consumer.poll(timeout_ms=BROKER_WAIT_TIMEOUT_MS).values()
    temp_consumer.commit()
    actual = [convert_message(message.value) for batch in batches for message in batch]
    assert actual == expected
