import asyncio as aio
from asyncio.events import get_running_loop
from lazyorm.connection import setup_elastic, setup_mqtt, setup_redis
from lazyorm.lmodel import build_model
from os import close
from lazyorm.lmqtt import connect_mqtt
from lazyorm.lelastic import connect_elastic
from lazyorm.lredis import connect_redis
import lazyorm as lorm
from lazyorm.logger import setDebugLevel

import logging
setDebugLevel(logging.DEBUG)


def test_dict():
    class Human(lorm.LDict):
        _cols = dict(
            id=str,
            name=str
        )

    h = Human()
    print(h)
    h = Human(id='001', name='hello')
    print(h)
    print(h.id)
    print(h.name)

    class Female(Human):
        _cols = dict(
            gender=lambda x=None: "female"
        )

    f = Female(id='002', name="mary")
    print(f)
    print(f.id)
    print(f.name)
    print(f.gender)

    class Girl(Female):
        _cols = dict(
            age=lambda x=None: "< 40"
        )

    g = Girl(id='003', name='lucy')
    print(g)
    print(g.id)
    print(g.name)
    print(g.age)
    print(g.gender)
    assert g.id == '003'
    assert g.gender == 'female'


def test_model():
    class Human(lorm.LObject):
        _cols = dict(
            id=str,
            name=str
        )
    h = Human()
    print(h)
    h = Human(id='001', name='hello')
    print(h)
    print(h.id)
    print(h.name)


def test_config():
    es_conf = lorm.ElasticConfig()
    rd_conf = lorm.RedisConfig()
    mq_conf = lorm.MqttConfig()
    print(es_conf)
    print(rd_conf)
    print(mq_conf)


def test_elastic():

    es = connect_elastic()
    assert es is None

    lorm.setup_elastic(host="localhost")

    es = connect_elastic()
    assert es is not None

    loop = lorm.get_loop()

    loop.run_until_complete(es.put('test', '{"id": 1}', doc_id='hello'))
    loop.run_until_complete(es.get('test', doc_id='hello1'))
    loop.run_until_complete(es.search('test', id=1))

    loop.run_until_complete(es.cli.close())


async def async_test_mqtt():
    mq = connect_mqtt()
    assert mq is None
    lorm.setup_mqtt(host="localhost", topics=['/test'])
    mq = connect_mqtt()
    assert mq is not None
    await mq.put('/test', b'test_msg')
    ret = await mq.get('/test')
    print(ret)


def test_mqtt():
    aio.run(async_test_mqtt())


def test_redis():
    rd = connect_redis()
    assert rd is None

    lorm.setup_redis(host="localhost")

    rd = connect_redis()
    assert rd is not None

    loop = lorm.get_loop()

    loop.run_until_complete(rd.set('test', 'test_value'))
    ret = loop.run_until_complete(rd.get('test'))
    assert ret == 'test_value'

    ret = loop.run_until_complete(rd.delete('test'))
    ret = loop.run_until_complete(rd.get('test'))

    assert ret is None

    loop.run_until_complete(rd.hset('test_hash', 'test', 'test_value'))
    ret = loop.run_until_complete(rd.hget('test_hash', 'test'))
    assert ret == 'test_value'
    ret = loop.run_until_complete(rd.hdelete('test_hash', 'test'))
    ret = loop.run_until_complete(rd.hget('test_hash', 'test'))
    assert ret is None

    ret = loop.run_until_complete(rd.rpush('test_queue', 'test'))

    ret = loop.run_until_complete(rd.lpop('test_queue'))
    assert ret is not None
    ret = loop.run_until_complete(rd.lpop('test_queue', timeout=1))
    assert ret is None


def test_model():

    class Test(build_model(False)):
        _es_index = 'test_index'
        _mq_topic = 'test_topic'
        _rd_queue = 'test_queue'

        _cols = dict(
            a=str,
            b=str,
            c=str
        )

    setup_elastic('localhost')
    setup_mqtt('localhost')
    setup_redis('localhost')

    t = Test()

    t.es_put('001')
    tt = Test.es_get('001')

    assert t == tt

    ttt = Test.es_get_n_cache('001')
    assert t == ttt
    tttt = Test.es_get_n_cache('001')
    assert tttt == ttt
    Test.es_del_n_cache('001')
    ttttt = Test.es_get_n_cache('001')
    assert ttttt is None


if __name__ == "__main__":
    # test_dict()
    # test_model()
    # test_config()
    # test_elastic()
    # test_mqtt()
    # test_redis()
    test_model()
