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
                    LOG.debug("%s could not get %s from base, try to get from get_connection", name, _attr_name)
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
    def mqtt_get(cls, **kw):
        if cls._mqtt is None:
            LOG.warning("mqtt is not initialized, will return None")
            return None
        return cls(**json.loads(cls._mqtt.get()))

    def mqtt_put(self):
        if self._mqtt is None:
            LOG.warning("%s: mqtt is not initialized, will not send %s", self.__class__.__name__, repr(self))
        else:
            self._mqtt.put(payload=json.dumps(self))
        return self

    @classmethod
    def es_search(cls, offset=0, page_size=10, **kw):
        return [cls(**ret) for ret in cls._elastic.search(offset, page_size, **kw)]

    @classmethod
    def es_get(cls, doc_id=None, **kw):
        ret = cls._elastic.get(doc_id=doc_id, **kw)
        if ret is None:
            return None
        return cls(**ret)

    def es_put(self, doc_id=None):
        return self._elastic.put(doc_id=doc_id, **self)


if __name__ == "__main__":

    import time
    import os

    es_node = os.environ['ES_NODE']
    es_username = os.environ['ES_USER']
    es_password = os.environ['ES_PASSWORD']

    get_connection('Human', 'mqtt', host='localhost', port=1883, topic='/test')
    get_connection('Human', 'elastic', index='test_index_lazy', es_node=es_node, es_username=es_username, es_password=es_password)

    _LOG = logging.getLogger('lazy.model.main')

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

    h = Human(id=100, name="john", age=64)

    _LOG.info("create human=%s", h)

    h.mqtt_put()

    _LOG.info("put is done")

    hh = Human.mqtt_get()

    _LOG.info("got human=%s", hh)

    hh.es_put()
    hhh = Human.es_search()
    hhh = Human.es_get(doc_id='FndU5FuY9WdELCTpqk4j3mSgtlJzcORebiLFiisZrYDeDN3R')
    print(hhh)
    hhh = Human.es_get(doc_id='AAAA')
    print(hhh)

    f = Female(id=1004, name="mary", age=13)
    print(f)
    f.es_put()
