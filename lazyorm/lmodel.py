import asyncio as aio
import json
from .ldict import LDict, LDictMeta
from .llog import getLogger
from .lnode import MQTTNode

LOG = getLogger('lazy.model')


async def _mq_put(self):
    if self._mq is None:
        LOG.warning("mqtt instance is none, ignore put")
        return self
    await self._mq.put(json.dumps(self))
    return self


@classmethod
async def _mq_get(cls, timeout=None):
    if cls._mq is None:
        LOG.warning("mqtt instance is none, ignore get")
        return None
    ret = await cls._mq.get(timeout=timeout)
    if ret is None:
        return None
    try:
        ret = json.loads(ret)
    except:
        LOG.error("could not decode json %s", ret)
        return None
    return cls(**ret)


def _s_mq_put(self):
    if self._mq.loop is None:
        raise ValueError("loop not available for sync mq_put")
    return self._mq.loop.run_until_complete(self._mq_put())


@classmethod
def _s_mq_get(cls, timeout=None):
    if cls._mq.loop is None:
        raise ValueError("loop not available for sync mq_get")
    return cls._mq.loop.run_until_complete(cls._mq_get(timeout=timeout))


def meta_append_mqtt_methods(name, attrs, is_async):

    assert isinstance(attrs, dict)
    assert isinstance(is_async, bool)

    if is_async:
        attrs['mq_put'] = _mq_put
        attrs['mq_get'] = _mq_get
    else:
        attrs['_mq_put'] = _mq_put
        attrs['_mq_get'] = _mq_get
        attrs['mq_put'] = _s_mq_put
        attrs['mq_get'] = _s_mq_get

    _mq = MQTTNode(name)

    if _mq is None:
        LOG.warning("model %s mqtt not initialzed", name)
    else:
        if _mq.loop is not None and is_async:
            LOG.warning("async model should not initialzed with external loop")
    attrs['_mq'] = _mq


def meta_append_elastic_methods(name, attrs, is_async):

    assert isinstance(attrs, dict)
    assert isinstance(is_async, bool)


def meta_append_redis_methods(name, attrs, is_async):
    assert isinstance(attrs, dict)
    assert isinstance(is_async, bool)


class LModelMeta(LDictMeta):
    def __new__(cls, name, bases, attrs):
        is_async = attrs['_async']

        for b in (b for b in bases if hasattr(b, '_async')):
            if b.__name__ == 'LModel':
                continue
            if b._async != is_async:
                if is_async:
                    raise ValueError("could not derive async model from sync model")
                else:
                    raise ValueError("could not derive sync model from async model")

        meta_append_mqtt_methods(name, attrs, is_async)
        meta_append_elastic_methods(name, attrs, is_async)
        meta_append_redis_methods(name, attrs, is_async)

        return LDictMeta.__new__(cls, name, bases, attrs)


class LModel(LDict, metaclass=LModelMeta):
    _cols = {}
    _async = True
    _mq = None
    _es = None
    _rd = None
