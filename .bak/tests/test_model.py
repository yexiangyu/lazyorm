
import time
import os
import logging

from lazyorm.lazyconnection import get_connection
from lazyorm.lazymodel import LazyModel


class Timer(object):
    def __init__(self):
        self.start = time.time()
        self.last = self.start

    def step(self):
        now = time.time()
        delta = now - self.last
        self.last = now
        return delta


def test_model():
    es_node = os.environ['ES_NODE']
    es_username = os.environ['ES_USERNAME']
    es_password = os.environ['ES_PASSWORD']
    redis_node = os.environ['REDIS_NODE']
    redis_password = os.environ['REDIS_PASSWORD']

    redis_host, redis_port = redis_node.split(":")
    redis_port = int(redis_port)

    mqtt_node = os.environ['MQTT_NODE']
    mqtt_host, mqtt_port = mqtt_node.split(":")
    mqtt_port = int(mqtt_port)

    tm = Timer()

    get_connection('Human', 'mqtt', host=mqtt_host, port=mqtt_port, topic='/test')
    get_connection('Human', 'elastic', index='test_index_lazy', es_node=es_node, es_username=es_username, es_password=es_password)
    get_connection('Human', 'redis', hash='human', host=redis_host, port=redis_port, password=redis_password)

    _LOG = logging.getLogger('lazy.model.main')

    _LOG.info("connection ready in %f", tm.step())

    class Human(LazyModel):
        _keys = dict(
            id=int,
            name=str,
            age=int
        )

    class Female(Human):
        _keys = dict(
            gender=lambda x=None: "female"
        )

    _LOG.info("model definition done, delta=%f", tm.step())

    h = Human(id=100, name="john", age=64)

    _LOG.info("create human=%s", h)

    tm.step()

    h.mq_put()

    _LOG.info("put %s to mqtt is done, delta=%f", h, tm.step())

    hh = Human.mq_get()

    _LOG.info("get human=%s from mqtt, delta=%f", hh, tm.step())

    hh.es_put()

    _LOG.info("put human=%s to es, delta=%f", hh, tm.step())

    hhh = Human.es_search()

    _LOG.info("search in human es return %d results, delta=%f", len(hhh), tm.step())

    hhh = Human.es_get(doc_id='FndU5FuY9WdELCTpqk4j3mSgtlJzcORebiLFiisZrYDeDN3R')

    _LOG.info('get FndU5FuY9WdELCTpqk4j3mSgtlJzcORebiLFiisZrYDeDN3R: %s, delta=%f', hhh, tm.step())

    hhh = Human.es_get(doc_id='AAAA')

    _LOG.info('get key AAAA: %s, delta=%.f', repr(hhh), tm.step())

    f = Female(id=1004, name="mary", age=13)

    _LOG.info("create Female: %s", f)

    tm.step()

    f.es_put()

    _LOG.info("Female: %s es put done delta=%f", f, tm.step())

    f.rd_put(f['id'])

    _LOG.info("Female: %s rd put done, delta=%f", f, tm.step())

    ret = Female.rd_get(1004)

    _LOG.info("Female: %s rd got done, delta=%f", ret, tm.step())
