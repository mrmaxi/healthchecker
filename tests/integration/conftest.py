from pytest import fixture

import broker
import db

# define settings default values
TOPIC_NAME = "test_topic"

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_CLIENT_ID = "CONSUMER_CLIENT_TEST"
KAFKA_GROUP_ID = "CONSUMER_GROUP_ID"

BROKER_WAIT_TIMEOUT_MS = 100

PG_CONNECTION_STR = 'postgres://postgres:mysecretpassword@127.0.0.1:5432/postgres'


# override settings by local values for integration tests
try:
    from .local_settings import *       # noqa: F401, F403
except ImportError:
    pass


broker.KAFKA_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS
broker.TOPIC_NAME = TOPIC_NAME
broker.KAFKA_CLIENT_ID = KAFKA_CLIENT_ID
broker.KAFKA_GROUP_ID = KAFKA_GROUP_ID

db.PG_CONNECTION_STR = PG_CONNECTION_STR


@fixture(scope='session')
def temp_producer():
    return broker.get_kafka_producer()


@fixture(scope='session')
def temp_consumer():
    return broker.get_kafka_consumer()


@fixture(scope='session')
def temp_admin():
    return broker.get_kafka_admin()


@fixture(scope='session')
def temp_conn():
    return db.get_connect()


def clear_queue(consumer):
    consumer.poll(timeout_ms=BROKER_WAIT_TIMEOUT_MS or 100).values()
    consumer.commit()


def clear_db(conn: db.Connection):
    curr = conn.cursor()
    sql = "delete from public.health_checks"
    curr.execute(sql)
    db.do_commit(conn)
