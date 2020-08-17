import json
import asyncio as aio
from lazyorm.connection import setup_mqtt
from .lmqtt import connect_mqtt
from .lredis import connect_redis
from .lelastic import connect_elastic
from .lloop import get_loop
from .ldict import LDict
from .logger import getLogger

LOG = getLogger("model")


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

        LOG.debug("check elastic=%s", cls._es is not None)

    @classmethod
    def _check_redis(cls):
        if cls._rd is None:
            cls._rd = connect_redis()

        if cls._rd is None:
            raise RuntimeError("rd not initialized")

        LOG.debug("check redis=%s", cls._rd is not None)

    @classmethod
    def _check_mqtt(cls):
        if cls._mq is None:
            config = setup_mqtt()
            if cls._mq_topic not in config.topics:
                LOG.debug("%s._mq_topic=%s not in topics list, added", cls.__name__, cls._mq_topic)
                config.topics.append(cls._mq_topic)
            cls._mq = connect_mqtt()

        if cls._mq is None:
            raise RuntimeError("mq not initialized")

        LOG.debug("check mqtt=%s", cls._mq is not None)

    async def _es_put(self, doc_id=None):
        self._check_elastic()
        LOG.debug("%s es putting doc_id=%s", self.__class__.__name__, doc_id)
        return await self._es.put(self._es_index, json.dumps(self), doc_id=doc_id)

    @classmethod
    async def _es_get(cls, doc_id=None, **kwargs):
        cls._check_elastic()
        LOG.debug("%s es getting doc_id=%s, kwargs=%s", cls.__name__, doc_id, kwargs)
        return await cls._es.get(cls._es_index, doc_id=doc_id, **kwargs)

    @classmethod
    async def _es_del(cls, doc_id=None, **kwargs):
        cls._check_elastic()
        LOG.debug("%s es deleting doc_id=%s, kwargs=%s", cls.__name__, doc_id, kwargs)
        return await cls._es.delete(cls._es_index, doc_id=doc_id, **kwargs)

    @classmethod
    async def _es_search(cls, offset=0, page_size=10, **kwargs):
        cls._check_elastic()
        LOG.debug("%s es searching offset=%d, page_size=%d, kwargs=%s", cls.__name__, offset, page_size, kwargs)
        return await cls._es.search(cls._es_index, offset, page_size, **kwargs)

    @classmethod
    async def _es_search_by_query(cls, query):
        cls._check_elastic()
        LOG.debug("%s es searching by query=%s", cls.__name__, query)
        return await cls._es.es_search_by_query(cls._es_index, query)

    async def _rd_set(self, key, expire=None):
        self._check_redis()
        LOG.debug("%s rd setting key=%s, expire=%s", self.__class__.__name__, key, expire)
        return await self._rd.set(key, json.dumps(self), expire=expire)

    @classmethod
    async def _rd_get(cls, key):
        cls._check_redis()
        LOG.debug("%s rd getting key=%s", cls.__name__, key)
        ret = await cls._rd.get(key)
        if ret is not None:
            ret = json.loads(ret)
        return ret

    @classmethod
    async def _rd_del(cls, key):
        cls._check_redis()
        LOG.debug("%s rd deleting key=%s", cls.__name__, key)
        return await cls._rd.delete(key)

    async def _rd_hset(self, key, **kwargs):
        self._check_redis()
        LOG.debug("%s rd hsetting key=%s", self.__name__, key)
        return await self._rd.hset(self._rd_hash, key, json.dumps(self), **kwargs)

    @classmethod
    async def _rd_hget(cls, key):
        cls._check_redis()
        LOG.debug("%s rd hgetting key=%s", cls.__name__, key)
        return await cls._rd.hget(cls._rd_hash, key)

    @classmethod
    async def _rd_hdel(cls, key):
        cls._check_redis()
        LOG.debug("%s rd hdeleting key=%s", cls.__name__, key)
        return await cls._rd.hdelete(cls._rd_hash, key)

    @classmethod
    async def _rd_lpop(cls, key, timeout=None):
        cls._check_redis()
        LOG.debug("%s rd lpoping key=%s, timeout=%s", cls.__name__, key, repr(timeout))
        return await cls._rd.hlpop(key, timeout=timeout)

    async def _rd_rpush(self, key):
        self._check_redis()
        LOG.debug("%s rd rpushing key=%s ", self.__class__.__name__, key)
        return await self._rd.rpush(key, json.dups(self))

    async def _mq_put(self):
        self._check_mqtt()
        LOG.debug("%s mq puting topic=%s ", self.__class__.__name__, self._mq_topic)
        return await self._mq.put(self._mq_topic, json.dumps(self))

    @classmethod
    async def _mq_get(cls, timeout=None):
        cls._check_mqtt()
        LOG.debug("%s mq getting topic=%s timeout=%s", cls.__name__, cls._mq_topic, timeout)
        ret = await cls._mq.get(cls._mq_topic, timeout=timeout)
        return json.loads(ret) if ret else None

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

    async def mq_put(self):
        return await self._mq_put()

    @classmethod
    async def mq_get(cls, timeout=None):
        ret = await cls._mq_get(timeout)
        return cls(**ret) if ret else None

    async def _es_put_n_cache(self, doc_id, expire=None):
        LOG.debug("puting and cache doc_id=%s, expire=%s", doc_id, expire)
        await self._es_put(doc_id)
        await self._rd_set(doc_id, expire)
        return self

    async def es_put_n_cache(self, doc_id, expire=None):
        return await self._es_put_n_cache(doc_id, expire)

    @classmethod
    async def _es_get_n_cache(cls, doc_id, expire=None):
        LOG.debug("getting and cache doc_id=%s, expire=%s", doc_id, expire)
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
        LOG.debug("deleting and cache doc_id=%s", doc_id)
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

    def mq_put(self):
        return self._loop.run_until_complete(self._mq_put())

    @classmethod
    def mq_get(cls, timeout=None):
        ret = cls._loop.run_until_complete(cls._mq_get(timeout))
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
