import json
import asyncio as aio
from lazyorm.connection import setup_mqtt
from .lmqtt import connect_mqtt
from .lredis import connect_redis
from .lelastic import connect_elastic
from .lloop import get_loop
from .ldict import LDict
from .logger import getLogger
from .lid import gen_id

LOG = getLogger("model")


class LObject(LDict):

    _es = None
    _es_index = None
    _es_refresh = '10s'

    _mq = None
    _mq_topic = None

    _rd = None
    _rd_hash = None
    _rd_queue = None

    _loop = get_loop()

    @classmethod
    async def _check_elastic(cls):
        if cls._es is None:
            cls._es = connect_elastic()
            if cls._es_index:
                try:
                    await cls._es.cli.indices.create(cls._es_index)
                except Exception as e:
                    pass
                except BaseException as e:
                    pass
                tmp_doc_id = gen_id()
                await cls().es_put(doc_id=tmp_doc_id)
                await cls.es_del(doc_id=tmp_doc_id)
                await cls._es.cli.indices.put_settings('{"settings": {"refresh_interval": "%s"}}' % (cls._es_refresh), index=cls._es_index)
                LOG.info("update index=%s refresh=%s", cls._es_index, cls._es_refresh)

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
        await self._check_elastic()
        LOG.debug("%s es putting doc_id=%s", self.__class__.__name__, doc_id)
        return await self._es.put(self._es_index, json.dumps(self), doc_id=doc_id)

    @classmethod
    async def _es_get(cls, doc_id=None, **kwargs):
        await cls._check_elastic()
        LOG.debug("%s es getting doc_id=%s, kwargs=%s", cls.__name__, doc_id, kwargs)
        return await cls._es.get(cls._es_index, doc_id=doc_id, **kwargs)

    @classmethod
    async def _es_del(cls, doc_id=None, **kwargs):
        await cls._check_elastic()
        LOG.debug("%s es deleting doc_id=%s, kwargs=%s", cls.__name__, doc_id, kwargs)
        return await cls._es.delete(cls._es_index, doc_id=doc_id, **kwargs)

    @classmethod
    async def _es_search(cls, offset=0, page_size=10, **kwargs):
        await cls._check_elastic()
        LOG.debug("%s es searching offset=%d, page_size=%d, kwargs=%s", cls.__name__, offset, page_size, kwargs)
        return await cls._es.search(cls._es_index, offset, page_size, **kwargs)

    @classmethod
    async def _es_search_by_query(cls, query):
        await cls._check_elastic()
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
        return await cls._rd.get(key)

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
    async def _rd_lpop(cls, queue=None, timeout=None):
        cls._check_redis()
        queue = queue if queue else cls._rd_queue
        LOG.debug("%s rd lpoping queue=%s, timeout=%s", cls.__name__, queue, repr(timeout))
        return await cls._rd.lpop(queue, timeout=timeout)

    async def _rd_rpush(self, queue=None):
        self._check_redis()
        queue = queue if queue else self._rd_queue
        LOG.debug("%s rd rpushing queue=%s ", self.__class__.__name__, queue)
        return await self._rd.rpush(queue, json.dumps(self))

    async def _mq_put(self, topic=None):
        self._check_mqtt()
        topic = topic if topic else self._mq_topic
        LOG.debug("%s mq puting topic=%s ", self.__class__.__name__, topic)
        return await self._mq.put(topic, json.dumps(self))

    @classmethod
    async def _mq_get(cls, topic=None, timeout=None):
        cls._check_mqtt()
        topic = topic if topic else cls._mq_topic
        LOG.debug("%s mq getting topic=%s timeout=%s", cls.__name__, topic, timeout)
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
        await cls._check_elastic()
        total, rets = await cls._es.search_by_query(cls._es_index, query)
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
    async def rd_lpop(cls, queue=None, timeout=None):
        ret = await cls._rd_lpop(queue, timeout)
        return cls(**ret) if ret else None

    async def rd_rpush(self, queue=None):
        return await self._rd_rpush(queue)

    async def mq_put(self, topic=None):
        return await self._mq_put(topic=topic)

    @classmethod
    async def mq_get(cls, topic=None, timeout=None):
        ret = await cls._mq_get(topic=topic, timeout=timeout)
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
    def rd_lpop(cls, queue=None, timeout=None):
        ret = cls._loop.run_until_complete(cls._rd_lpop(queue, timeout))
        return cls(**ret) if ret else None

    def rd_rpush(self, queue=None):
        return self._loop.run_until_complete(self._rd_rpush(queue))

    def mq_put(self, topic=None):
        return self._loop.run_until_complete(self._mq_put(topic))

    @classmethod
    def mq_get(cls, topic=None, timeout=None):
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

    def __del__(self):
        if self._es.cli:
            self._loop.run_until_complete(self._es.cli.close())
            self._es.cli = None
            LOG.debug("close elasticsearch")

        if self._rd.cli:
            self._rd.cli.close()
            self._loop.run_until_complete(self._rd.cli.wait_closed())
            self._rd.cli = None
            LOG.debug("close redis")

        if self._mq.cli:
            self._mq.cli = None
            LOG.debug("close mqtt")


def build_model(is_async=True):
    return LObject if is_async else SLObject
