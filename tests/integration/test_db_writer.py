from pytest import mark, param, fixture

import broker
from broker import create_topic_if_not_exists
from db_writer import write_once

from .conftest import clear_queue, BROKER_WAIT_TIMEOUT_MS
from .test_db import clear_db, get_record, TEST_RECORD1


@fixture(autouse=True, scope='module')
def my_setup_module(temp_consumer, temp_conn, temp_admin):
    create_topic_if_not_exists(admin=temp_admin, topic_name=broker.TOPIC_NAME)
    clear_queue(temp_consumer)
    clear_db(temp_conn)


@mark.parametrize("record", [
    param(TEST_RECORD1, id='message1'),
])
def test_writer(temp_producer, temp_consumer, temp_conn, record: dict):
    """Check that db_writer read messages from broker and write them into db"""

    assert not get_record(temp_conn, record['id']), "Isn't clear database, record already exists"

    message = {**record, 'dt': record['dt'].isoformat()}
    temp_producer.send(broker.TOPIC_NAME, message).get()

    # call db_writer
    write_once(temp_consumer, temp_conn, timeout_ms=BROKER_WAIT_TIMEOUT_MS)
    temp_conn.rollback()

    actual = get_record(temp_conn, record['id'])
    assert actual == record
