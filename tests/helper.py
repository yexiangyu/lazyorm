import os
import random
import asyncio as aio

from lazyorm.lmodel import LModel
from lazyorm.llog import getLogger
from lazyorm.lnode import (init_elastic, init_mqtt, init_redis)
from lazyorm.lid import lid

from faker import Faker

fake = Faker(['zh_CN', 'en_US'])

LOG = getLogger('tests')

es_node = os.environ['ES_NODE']
es_username = os.environ['ES_USERNAME']
es_password = os.environ['ES_PASSWORD']

es_host, es_port = es_node.split(":")
es_port = int(es_port)

LOG.info("elastic_node=%s:%d, u=%s,p=%s", es_host, es_port, es_username, es_password)

redis_node = os.environ['REDIS_NODE']
redis_password = os.environ['REDIS_PASSWORD']


redis_host, redis_port = redis_node.split(":")
redis_port = int(redis_port)

LOG.info("redis_node=%s:%d, p=%s", redis_host, redis_port, redis_password)

mqtt_node = os.environ['MQTT_NODE']
mqtt_host, mqtt_port = mqtt_node.split(":")
mqtt_port = int(mqtt_port)

LOG.info("mqtt_node=%s:%d", mqtt_host, mqtt_port)


def init_connection(is_async=False):
    loop = None if is_async else aio.get_event_loop()
    init_elastic('Human', es_host, es_port, 'human_record', username=es_username, password=es_password, loop=loop)
    init_mqtt('Human', mqtt_host, mqtt_port, '/human_record', client_id=lid(), loop=loop)
    return loop


def create_class_n_loop(is_async=False):

    loop = init_connection(is_async)

    class Human(LModel):
        _async = is_async
        _cols = dict(
            id=lambda x=None: lid() if x is None else str(x),
            age=lambda x=None: random.randint(0, 100) if x is None else int(x),
            gender=lambda x=None: random.choice(['male', 'female', 'unknown']) if x is None else ('unknown' if x not in ['male', 'female', 'unknown'] else x),
            name=lambda x=None: fake.name() if x is None else str(x),
            address=lambda x=None: fake.address() if x is None else str(x)
        )

    LOG.info("Human... always async=%s", is_async)
    return loop, Human
