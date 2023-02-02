import logging

from kafka.consumer.fetcher import ConsumerRecord

from broker import get_kafka_consumer, KafkaConsumer
import db


log = logging.getLogger('app')

# define settings default values
LOG_LEVEL = logging.DEBUG

# override settings by local values
try:
    from conf.local_settings import *       # noqa: F401, F403
except ImportError:
    pass


def parse_message(message: ConsumerRecord) -> dict:
    fields = ['id', 'check_name', 'dt', 'health', 'status', 'duration', 'length', 'sample']
    mandatory = ['id', 'check_name', 'dt', 'health']
    if message.value and all([k in message.value for k in mandatory]):
        return {k: message.value.get(k) for k in fields}
    log.warning(f'skip message {message.partition} {message.offset} because not all the fields exists {message.value}')


def write_once(consumer: KafkaConsumer, conn: db.Connection, timeout_ms=None):
    timeout_ms = timeout_ms or 100
    batches = consumer.poll(timeout_ms=timeout_ms).values()
    for batch in batches:
        log.debug(f"got batch with {len(batch)} messages")
        for message in batch:
            rec = parse_message(message)
            if rec:
                db.append_record(conn, rec)

    if batches:
        db.do_commit(conn)
        consumer.commit()


def write_forever(consumer: KafkaConsumer, conn: db.Connection, timeout_ms=None):
    while True:
        write_once(consumer, conn, timeout_ms=timeout_ms)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
    log.setLevel(LOG_LEVEL)

    conn = db.get_connect()
    consumer = get_kafka_consumer()
    write_forever(consumer=consumer, conn=conn, timeout_ms=5000)
