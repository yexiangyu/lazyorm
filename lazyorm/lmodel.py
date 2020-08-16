import asyncio as aio
from .ldict import LDict, LDictMetaClass
from .lid import gen_id
from .lelastic import connect_elastic
from .lmqtt import connect_mqtt
from .lredis import connect_redis


class LModelMetaClass(LDictMetaClass):
    def __new__(cls, name, bases, attrs):

        if attrs['_es'] is None:
            attrs['_es'] = connect_elastic()

        if attrs['_mq'] is None:
            attrs['_mq'] = connect_mqtt()

        if attrs['_rd'] is None:
            attrs['_rd'] = connect_redis()

        return LDictMetaClass.__new__(cls, name, bases, attrs)


class LObject(LDict, metaclass=LModelMetaClass):

    _async = True

    _cols = dict(
        _id=lambda x=None: gen_id() if x is None else str(x)
    )

    _es = None
    _es_index = None

    _mq = None
    _mq_topic = None
    _mq_queue = None

    _rd = None
    _rd_hash = None
    _rd_queue = None

    def __init__(self, **kwargs):
        pass

    async def es_put(self, key=None):
        if key is None:
            key = self._id

    @classmethod
    async def es_get(cls, id):
        pass

    @classmethod
    async def es_del(cls, id):
        pass

    @classmethod
    async def es_simple_search(cls, offset=0, page_size=32, **kwargs):
        pass

    @classmethod
    async def es_search(cls, query):
        pass

    async def rd_set(self, key=None, expire=None):
        if key is None:
            key = self._id

    @classmethod
    async def rd_get(self, key):
        pass

    @classmethod
    async def rd_del(cls, key):
        pass

    async def rd_hset(self, key=None):
        if key is None:
            key = self._id

    @classmethod
    async def rd_hget(cls, key):
        pass

    @classmethod
    async def rd_hdel(cls, key):
        pass

    async def mq_put(self, key=None):
        if key is None:
            key = self._id

    @classmethod
    def mq_get(self, key, block=True):
        pass
