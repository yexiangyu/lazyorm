import json
from .llog import getLogger
from .lnode_redis import RedisNode

LOG = getLogger('lazy.redis')


async def _rd_set(self, key, **kwargs):
    assert self._rd
    await self._rd.set(key, json.dumps(self), **kwargs)
    return self


def _s_rd_set(self, key, **kwargs):
    return self._rd.loop.run_until_complete(self._rd_set(key, **kwargs))


@classmethod
async def _rd_get(cls, key):
    assert cls._rd
    ret = await cls._rd.get(key)
    if ret is None:
        return None
    ret = json.loads(ret)
    return cls(**ret)


@classmethod
def _s_rd_get(cls, key):
    return cls._rd.loop.run_until_complete(cls._rd_get(key))


@classmethod
async def _rd_del(cls, key):
    return await cls._rd.delete(key)


@classmethod
def _s_rd_del(cls, key):
    return cls._rd.loop.run_until_complete(cls._rd_del(key))


async def _rd_hset(self, key, **kwargs):
    assert self._rd
    await self._rd.hset(key, json.dumps(self), **kwargs)
    return self


def _s_rd_hset(self, key, **kwargs):
    return self._rd.loop.run_until_complete(self._rd_hset(key, **kwargs))


@classmethod
async def _rd_hget(cls, key):
    assert cls._rd
    ret = await cls._rd.hget(key)
    if ret is None:
        return None
    ret = json.loads(ret)
    return cls(**ret)


@classmethod
def _s_rd_hget(cls, key):
    return cls._rd.loop.run_until_complete(cls._rd_hget(key))


@classmethod
async def _rd_hdel(cls, key):
    return await cls._rd.hdel(key)


@classmethod
def _s_rd_hdel(cls, key):
    return cls._rd.loop.run_until_complete(cls._rd_hdel(key))


@classmethod
async def _rd_lpop(cls, block=True):
    ret = await cls._rd.lpop(block=block)
    if ret is None:
        return None
    ret = json.loads(ret[1]) if block else json.loads(ret)
    return cls(**ret)


@classmethod
def _s_rd_lpop(cls, block=True):
    return cls._rd.loop.run_until_complete(cls._rd_lpop(block=block))


async def _rd_rpush(self, block=True):
    await self._rd.rpush(json.dumps(self),  block=block)
    return self


def _s_rd_rpush(self, block=True):
    return self._rd.loop.run_until_complete(self._rd_rpush(block=block))


def meta_append_redis_methods(name, attrs, is_async):
    assert isinstance(attrs, dict)
    assert isinstance(is_async, bool)

    if is_async:
        attrs['rd_set'] = _rd_set
        attrs['rd_get'] = _rd_get
        attrs['rd_del'] = _rd_del

        attrs['rd_hset'] = _rd_hset
        attrs['rd_hget'] = _rd_hget
        attrs['rd_hdel'] = _rd_hdel

        attrs['rd_lpop'] = _rd_lpop
        attrs['rd_rpush'] = _rd_rpush

    else:

        attrs['_rd_set'] = _rd_set
        attrs['_rd_get'] = _rd_get
        attrs['_rd_del'] = _rd_del
        attrs['rd_set'] = _s_rd_set
        attrs['rd_get'] = _s_rd_get
        attrs['rd_del'] = _s_rd_del

        attrs['_rd_hset'] = _rd_hset
        attrs['_rd_hget'] = _rd_hget
        attrs['_rd_hdel'] = _rd_hdel
        attrs['rd_hset'] = _s_rd_hset
        attrs['rd_hget'] = _s_rd_hget
        attrs['rd_hdel'] = _s_rd_hdel

        attrs['_rd_lpop'] = _rd_lpop
        attrs['_rd_rpush'] = _rd_rpush
        attrs['rd_lpop'] = _s_rd_lpop
        attrs['rd_rpush'] = _s_rd_rpush

    _rd = RedisNode(name)

    if _rd is None:
        if name != 'LModel':
            LOG.warning("model %s es not initialzed", name)
    else:
        if _rd.loop is not None and is_async:
            LOG.warning("async model should not initialzed with external loop")
    attrs['_rd'] = _rd
