import json
import logging

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic


# define settings default values
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "health_checker"
KAFKA_CLIENT_ID = "CONSUMER_CLIENT_ID1"
KAFKA_GROUP_ID = "CONSUMER_GROUP_ID"

# override settings by local values
try:
    from conf.local_settings import *       # noqa: F401, F403
except ImportError:
    pass


log = logging.getLogger('app.broker')


def create_topic_if_not_exists(admin: KafkaAdminClient, topic_name: str):
    if topic_name not in admin.list_topics():
        log.warning(f'create topic {topic_name}')
        admin.create_topics(new_topics=[NewTopic(name=topic_name, num_partitions=1, replication_factor=2)])


def get_kafka_admin() -> KafkaAdminClient:
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id=KAFKA_CLIENT_ID,
        security_protocol="SSL",
        ssl_cafile="conf/ca.pem",
        ssl_certfile="conf/service.cert",
        ssl_keyfile="conf/service.key",
    )
    return admin


def get_kafka_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SSL",
        ssl_cafile="conf/ca.pem",
        ssl_certfile="conf/service.cert",
        ssl_keyfile="conf/service.key",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer


def get_kafka_consumer() -> KafkaConsumer:
    log.info(f'try connect to kafka topic {TOPIC_NAME}')
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id=KAFKA_CLIENT_ID,
        group_id=KAFKA_GROUP_ID,
        security_protocol="SSL",
        ssl_cafile="conf/ca.pem",
        ssl_certfile="conf/service.cert",
        ssl_keyfile="conf/service.key",
        value_deserializer=lambda data: json.loads(data.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
    )
    # make first poll to assign partitions
    consumer.poll()
    assigned_topics = ', '.join([f'{tp.topic}:{tp.partition}' for tp in consumer.assignment()])
    log.info(f'connected to kafka to topics: {assigned_topics}')
    return consumer
