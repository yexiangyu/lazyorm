import json
import asyncio as aio
from lazyorm.lmqtt import connect_mqtt
from lazyorm.lredis import connect_redis
from lazyorm.lelastic import connect_elastic
from lazyorm.connection import setup_elastic, setup_mqtt, setup_redis
from lazyorm.lloop import get_loop
from .ldict import LDict


class LObject(LDict):

    _es = None
    _es_index = None

    _mq = None
    _mq_topic = None

    _rd = None
    _rd_hash = None
    _rd_queue = None

    _loop = get_loop()

    @classmethod
    def _check_elastic(cls):
        if cls._es is None:
            cls._es = connect_elastic()

        if cls._es is None:
            raise RuntimeError("es not initialized")

    @classmethod
    def _check_redis(cls):
        if cls._rd is None:
            cls._rd = connect_redis()

        if cls._rd is None:
            raise RuntimeError("rd not initialized")

    @classmethod
    def _check_mqtt(cls):
        if cls._mq is None:
            cls._mq = connect_mqtt()

        if cls._mq is None:
            raise RuntimeError("mq not initialized")

    async def _es_put(self, doc_id=None):
        self._check_elastic()
        return await self._es.put(self._es_index, json.dumps(self), doc_id=doc_id)

    @classmethod
    async def _es_get(cls, doc_id=None, **kwargs):
        cls._check_elastic()
        return await cls._es.get(cls._es_index, doc_id=doc_id, **kwargs)

    @classmethod
    async def _es_del(cls, doc_id=None, **kwargs):
        cls._check_elastic()
        return await cls._es.delete(cls._es_index, doc_id=doc_id, **kwargs)

    @classmethod
    async def _es_search(cls, offset=0, page_size=10, **kwargs):
        cls._check_elastic()
        return await cls._es.search(cls._es_index, offset, page_size, **kwargs)

    @classmethod
    async def _es_search_by_query(cls, query):
        cls._check_elastic()
        return await cls._es.es_search_by_query(cls._es_index, query)

    async def _rd_set(self, key, expire=None):
        self._check_redis()
        return await self._rd.set(key, json.dumps(self), expire=expire)

    @classmethod
    async def _rd_get(cls, key):
        cls._check_redis()
        ret = await cls._rd.get(key)

        if ret is not None:
            ret = json.loads(ret)
        return ret

    @classmethod
    async def _rd_del(cls, key):
        cls._check_redis()
        return await cls._rd.delete(key)

    async def _rd_hset(self, key, **kwargs):
        self._check_redis()
        return await self._rd.hset(self._rd_hash, key, json.dumps(self), **kwargs)

    @classmethod
    async def _rd_hget(cls, key):
        cls._check_redis()
        return await cls._rd.hget(cls._rd_hash, key)

    @classmethod
    async def _rd_hdel(cls, key):
        cls._check_redis()
        return await cls._rd.hdelete(cls._rd_hash, key)

    @classmethod
    async def _rd_lpop(cls, key, timeout=None):
        cls._check_redis()
        return await cls._rd.hlpop(key, timeout=timeout)

    async def _rd_rpush(self, key):
        self._check_redis()
        return await self._rd.rpush(key, json.dups(self))

    async def _mq_put(self, topic):
        self._check_mqtt()
        return await self._mq.put(topic, json.dumps(self))

    @classmethod
    async def _mq_get(cls, topic, timeout=None):
        cls._check_mqtt()
        return await cls._mq.get(topic, timeout=timeout)

    # wrap result

    async def es_put(self, doc_id=None):
        return await self._es_put(doc_id)

    @classmethod
    async def es_get(cls, doc_id=None, **kwargs):
        ret = await cls._es_get(doc_id, **kwargs)
        return cls(**ret) if ret else None

    @classmethod
    async def es_del(cls, doc_id=None, **kwargs):
        await cls._es_del(doc_id, **kwargs)

    @classmethod
    async def es_search(cls, offset=0, page_size=10, **kwargs):
        total, rets = await cls._es_search(offset, page_size, **kwargs)
        return total, [cls(**ret) for ret in rets]

    @classmethod
    async def es_search_by_query(cls, query):
        cls._check_es_connection()
        total, rets = await cls._es.es_search_by_query(cls._es_index, query)
        return total, [cls(**ret) for ret in rets]

    async def rd_set(self, key, expire=None):
        return self._rd_set(key, expire)

    @classmethod
    async def rd_get(cls, key):
        ret = await cls._rd_get(key)
        return cls(**ret) if ret else None

    @classmethod
    async def rd_del(cls, key):
        return await cls._rd_del(key)

    async def rd_hset(self, key, **kwargs):
        return await self._rd_set(key, **kwargs)

    @classmethod
    async def rd_hget(cls, key):
        ret = await cls._rd_hget(key)
        return cls(**ret) if ret else None

    @classmethod
    async def rd_hdel(cls, key):
        return await cls._rd_del(key)

    @classmethod
    async def rd_lpop(cls, key, timeout=None):
        ret = await cls._rd_lpop(key, timeout)
        return cls(**ret) if ret else None

    async def rd_rpush(self, key):
        return await self._rd_rpush(key)

    async def mq_put(self, topic):
        return await self._mq_put(topic)

    @classmethod
    async def mq_get(cls, topic, timeout=None):
        ret = await cls._mq_get(topic, timeout)
        return cls(**ret) if ret else None

    async def _es_put_n_cache(self, doc_id, expire=None):
        await self._es_put(doc_id)
        await self._rd_set(doc_id, expire)
        return self

    async def es_put_n_cache(self, doc_id, expire=None):
        return await self._es_put_n_cache(doc_id, expire)

    @classmethod
    async def _es_get_n_cache(cls, doc_id, expire=None):
        ret = await cls._rd_get(doc_id)
        if ret is None:
            ret = await cls._es_get(doc_id)
            if ret is not None:
                ret = cls(**ret)
                await ret._rd_set(doc_id, expire=expire)

        return ret

    @classmethod
    async def es_get_n_cache(cls, doc_id, expire=None):
        ret = await cls._es_get_n_cache(doc_id, expire)
        return cls(**ret) if ret else None

    @classmethod
    async def _es_del_n_cache(cls, doc_id):
        await cls._es_del(doc_id)
        await cls._rd_del(doc_id)

    @classmethod
    async def es_del_n_cache(cls, doc_id):
        await cls._es_del_n_cache(doc_id)


class SLObject(LObject):

    def es_put(self, doc_id=None):
        return self._loop.run_until_complete(self._es_put(doc_id))

    @classmethod
    def es_get(cls, doc_id=None, **kwargs):
        ret = cls._loop.run_until_complete(cls._es_get(doc_id, **kwargs))
        return cls(**ret) if ret else None

    @classmethod
    def es_del(cls, doc_id=None, **kwargs):
        return cls._loop.run_until_complete(cls._es_del(doc_id, **kwargs))

    @classmethod
    def es_search(cls, offset=0, page_size=10, **kwargs):
        total, rets = cls._loop.run_until_complete(cls._es_search(offset, page_size, **kwargs))
        return total, [cls(**ret) for ret in rets]

    @classmethod
    def es_search_by_query(cls, query):
        cls._check_es_connection()
        total, rets = cls._loop(cls._es.es_search_by_query(cls._es_index, query))
        return total, [cls(**ret) for ret in rets]

    def rd_set(self, key, expire=None):
        return self._loop.run_until_complete(self._rd_set(key, expire))

    @classmethod
    def rd_get(cls, key):
        ret = cls._loop.run_until_complete(cls._rd_get(key))
        return cls(**ret) if ret else None

    @classmethod
    def rd_del(cls, key):
        return cls._loop.run_until_complete(cls._rd_del(key))

    def rd_hset(self, key, **kwargs):
        return self._loop.run_until_complete(self._rd_set(key, **kwargs))

    @classmethod
    def rd_hget(cls, key):
        ret = cls._loop.run_until_complete(cls._rd_hget(key))
        return cls(**ret) if ret else None

    @classmethod
    def rd_hdel(cls, key):
        return cls._loop.run_until_complete(cls._rd_del(key))

    @classmethod
    def rd_lpop(cls, key, timeout=None):
        ret = cls._loop.run_until_complete(cls._rd_lpop(key, timeout))
        return cls(**ret) if ret else None

    def rd_rpush(self, key):
        return self._loop.run_until_complete(self._rd_rpush(key))

    def mq_put(self, topic):
        return self._loop.run_until_complete(self._mq_put(topic))

    @classmethod
    def mq_get(cls, topic, timeout=None):
        ret = cls._loop.run_until_complete(cls._mq_get(topic, timeout))
        return cls(**ret) if ret else None

    def es_put_n_cache(self, doc_id, expire=None):
        return self._loop.run_until_complete(self._es_put_n_cache(self, doc_id, expire))

    @classmethod
    def es_get_n_cache(cls, doc_id, expire=None):
        ret = cls._loop.run_until_complete(cls._es_get_n_cache(doc_id, expire))
        return cls(**ret) if ret else None

    @classmethod
    def es_del_n_cache(cls, doc_id):
        return cls._loop.run_until_complete(cls._es_del_n_cache(doc_id))


def build_model(is_async=True):
    return LObject if is_async else SLObject
