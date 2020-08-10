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

        if '_keys' in attrs:
            _keys = attrs['_keys']
            for b in (b for b in bases if hasattr(b, '_keys')):
                for k, vt in b._keys.items():
                    if k in _keys:
                        continue
                    _keys[k] = vt
            attrs['_keys'] = _keys
        return type.__new__(cls, name, bases, attrs)


class LazyModel(LazyDict, metaclass=LazyModelMeta):
    _mqtt = None
    _redis = None
    _elastic = None

    @classmethod
    def mq_get(cls):
        """get msg from mqtt and loads to Model

        Returns:
            LazyModel:
        """

        if cls._mqtt is None:
            LOG.warning("mqtt is not initialized, will return None")
            return None
        return cls(**json.loads(cls._mqtt.get()))

    def mq_put(self):
        if self._mqtt is None:
            LOG.warning("%s: mqtt is not initialized, will not send %s", self.__class__.__name__, repr(self))
        else:
            self._mqtt.put(json.dumps(self))
        return self

    @classmethod
    def es_search(cls, offset=0, page_size=10, **kw):
        """search in elasticsearch

        Args:
            offset (int, optional): [offset to return]. Defaults to 0.
            page_size (int, optional): [nums of result each search]. Defaults to 10.

        Returns:
            list of cls: if no more result to go, empty list like: [] will return
        """
        return [cls(**ret) for ret in cls._elastic.search(offset, page_size, **kw)]

    @classmethod
    def es_get(cls, doc_id=None, **kw):
        """get from elasticsearch

        Args:
            doc_id (str, optional): [get a known doc_id]. Defaults to None.
            kw: to search, only equals filter now available

        Returns:
            [cls]: cls or None
        """
        ret = cls._elastic.get(doc_id=doc_id, **kw)
        if ret is None:
            return None
        return cls(**ret)

    def es_put(self, doc_id=None):
        """put self to es

        will create a lazyid if no doc_id is provide

        """
        return self._elastic.put(json.dumps(self), doc_id=doc_id)

    @classmethod
    def es_delete(cls, doc_id=None, **kw):
        LOG.warning("will not delete anything from es currently")

    @classmethod
    def rd_get(cls, key):
        ret = cls._redis.get(key)
        if ret is None:
            return ret
        return cls(**json.loads(ret))

    def rd_put(self, key):
        self._redis.put(key, json.dumps(self))

    @classmethod
    def rd_delete(self, key):
        LOG.warning("will not delete anything from rd currently")


if __name__ == "__main__":

    class Timer(object):
        def __init__(self):
            self.start = time.time()
            self.last = self.start

        def step(self):
            now = time.time()
            delta = now - self.last
            self.last = now
            return delta

    import time
    import os

    es_node = os.environ['ES_NODE']
    es_username = os.environ['ES_USERNAME']
    es_password = os.environ['ES_PASSWORD']
    redis_node = os.environ['REDIS_NODE']
    redis_password = os.environ['REDIS_PASSWORD']

    redis_host, redis_port = redis_node.split(":")
    redis_port = int(redis_port)

    tm = Timer()

    get_connection('Human', 'mqtt', host='localhost', port=1883, topic='/test')
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
