import asyncio as aio
from asyncio.events import get_running_loop
from lazyorm.lmqtt import connect_mqtt
from lazyorm.lelastic import connect_elastic
import lazyorm as lorm


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

    loop.run_until_complete(es.index('test', '{"id": 1}'))

    async def get_loop():
        _loop = aio.get_running_loop()
        print("current running loop_id=", id(_loop))
    loop.run_until_complete(get_loop())
    loop.run_until_complete(es.close())


def test_mqtt():
    mq = connect_mqtt()
    assert mq is None
    lorm.setup_mqtt(host="localhost")
    mq = connect_mqtt()
    assert mq is not None


if __name__ == "__main__":
    # test_dict()
    # test_model()
    # test_config()
    # test_elastic()
    test_mqtt()
