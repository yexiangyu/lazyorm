from .lazyconnection import get_connection
from .lazydict import LazyDict, LazyDictMeta
from .lazyid import lazyid
import logging
import json

LOG = logging.getLogger('lazy.model')


def extract_attr_from_bases(key, bases):
    for base in bases:
        if hasattr(base, key):
            return getattr(base, key)

    return None


class LazyModelMeta(LazyDictMeta):

    def __new__(cls, name, bases, attrs):
        if name != 'LazyModel':
            for svc in ['mqtt', 'redis', 'elastic']:
                _attr_name = f"_{svc}"
                _attr = extract_attr_from_bases(_attr_name, bases)
                if _attr is None:
                    _attr = get_connection(name, svc)
                attrs[_attr_name] = _attr

        return LazyDictMeta.__new__(cls, name, bases, attrs)


class LazyModel(LazyDict, metaclass=LazyModelMeta):
    _mqtt = None
    _redis = None
    _elastic = None

    @classmethod
    async def mq_get(cls):
        """get msg from mqtt and loads to Model

        Returns:
            LazyModel:
        """

        if cls._mqtt is None:
            LOG.warning("mqtt is not initialized, will return None")
            return None
        return cls(**json.loads(await cls._mqtt.get()))

    async def mq_put(self):
        if self._mqtt is None:
            LOG.warning("%s: mqtt is not initialized, will not send %s", self.__class__.__name__, repr(self))
        else:
            await self._mqtt.put(json.dumps(self))
        return self

    @classmethod
    async def es_search(cls, offset=0, page_size=10, **kw):
        """search in elasticsearch

        Args:
            offset (int, optional): [offset to return]. Defaults to 0.
            page_size (int, optional): [nums of result each search]. Defaults to 10.

        Returns:
            list of cls: if no more result to go, empty list like: [] will return
        """
        return [cls(**ret) for ret in await cls._elastic.search(offset, page_size, **kw)]

    @classmethod
    async def es_get(cls, doc_id=None, **kw):
        """get from elasticsearch

        Args:
            doc_id (str, optional): [get a known doc_id]. Defaults to None.
            kw: to search, only equals filter now available

        Returns:
            [cls]: cls or None
        """
        ret = await cls._elastic.get(doc_id=doc_id, **kw)
        if ret is None:
            return None
        return cls(**ret)

    async def es_put(self, doc_id=None):
        """put self to es

        will create a lazyid if no doc_id is provide

        """
        return await self._elastic.put(json.dumps(self), doc_id=doc_id)
        return self

    @classmethod
    async def es_delete(cls, doc_id=None, **kw):
        LOG.warning("will not delete anything from es currently")

    @classmethod
    async def rd_get(cls, key):
        ret = await cls._redis.get(key)
        if ret is None:
            return ret
        return cls(**json.loads(ret))

    async def rd_put(self, key):
        await self._redis.put(key, json.dumps(self))
        return self

    @classmethod
    async def rd_delete(self, key):
        LOG.warning("will not delete anything from rd currently")
