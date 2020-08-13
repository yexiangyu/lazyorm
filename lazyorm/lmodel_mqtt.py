import asyncio as aio
import json
from .ldict import LDict, LDictMeta
from .llog import getLogger
from .lnode_mqtt import MQTTNode

LOG = getLogger('lazy.model')


async def _mq_put(self):
    assert self._mq
    await self._mq.put(json.dumps(self))
    return self


@classmethod
async def _mq_get(cls, timeout=None):
    assert cls._mq

    ret = await cls._mq.get(timeout=timeout)

    if ret is None:
        return None

    try:
        ret = json.loads(ret)
    except Exception as e:
        LOG.error("could not decode json %s", ret)
        raise e

    return cls(**ret)


def _s_mq_put(self):
    assert sef._mq.loop
    return self._mq.loop.run_until_complete(self._mq_put())


@classmethod
def _s_mq_get(cls, timeout=None):
    assert cls._mq.loop
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
        if name != 'LModel':
            LOG.warning("model %s mqtt not initialzed", name)
    else:
        if _mq.loop is not None and is_async:
            LOG.warning("async model should not initialzed with external loop")
    attrs['_mq'] = _mq
