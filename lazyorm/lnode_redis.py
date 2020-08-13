from .lid import lid
from .llog import getLogger
import asyncio as aio
import aioredis as redis

LOG = getLogger('lazy.redis')


class _RedisNode(object):
    def __init__(self, name, host='localhost', port=7379, hash=None, topic=None, password=None, db=0, loop=None):
        self.name = name
        self.host = host
        self.port = port
        self.hash = hash
        self.topic = topic
        self.password = password
        self.db = db
        self.loop = loop

        assert self.host and isinstance(self.host, str), self.host
        assert self.port and isinstance(self.port, int) and self.port > 0, self.port
        assert self.hash is None or isinstance(self.hash, str), self.hash
        assert self.topic is None or isinstance(self.topic, str), self.topic
        assert isinstance(self.db, int) and 0 <= self.db <= 16, self.db

        self.async_connected = False
        self.cli = None
        self.channel = None
        self.queue = None
        LOG.info("redis client %s initialzed, host=%s, port=%d, topic=%s", self.name, self.host, self.port, repr(self.topic))

    async def _async_init(self):
        if self.async_connected:
            return

        if self.loop is None:
            self.loop = aio.get_event_loop()

        self.cli = await redis.create_redis(
            f"redis://{self.host}:{self.port}",
            db=self.db,
            password=self.password,
            loop=self.loop
        )

        self.async_connected = True

    async def get(self, key):
        await self._async_init()
        ret = await self.cli.get(key)
        return ret

    async def set(self, key, value, **kw):
        await self._async_init()
        ret = await self.cli.set(key, value, **kw)
        return ret

    async def delete(self, key):
        await self._async_init()
        ret = await self.cli.delete(key)
        return

    # async def rm(self, key):
    #     await self._async_init()
    #     ret = await self.cli.del(key)
    #     return ret

    async def hget(self, key, hash=None):
        await self._async_init()
        hash = hash or self.hash
        assert hash
        ret = await self.cli.hget(hash, key)
        return ret

    async def hset(self, key, value, hash=None, **kwargs):
        await self._async_init()
        hash = hash or self.hash
        assert hash
        ret = await self.cli.hset(hash, key, value)
        return ret

    async def hdel(self, key, hash=None):
        await self._async_init()
        hash = hash or self.hash
        assert hash
        ret = await self.cli.hdel(hash, key)
        return ret

    async def lpop(self, block=True):
        await self._async_init()
        if block:
            ret = await self.cli.blpop(self.topic)
        else:
            ret = await self.cli.lpop(self.topic)
        return ret

    async def rpush(self, data, block=True):
        await self._async_init()
        if block:
            ret = await self.cli.rpush(self.topic, data)
        else:
            ret = await self.cli.brpush(self.topic, data)
        return ret


class RedisNode(object):
    _instances = {}

    def __new__(cls, name, **kwargs):
        if name not in cls._instances:
            if not kwargs:
                return None
            loop = kwargs.get('loop')
            LOG.debug("create redis node instance %s: loop=%s", name, 'none' if loop is None else id(loop))
            cls._instances[name] = _RedisNode(name, **kwargs)

        inst = cls._instances[name]

        return inst


def init_redis(name, host, port, password=None, topic=None, hash=None, loop=None, db=0):
    RedisNode(name, host=host, port=port, hash=hash, topic=topic, password=password, db=0, loop=loop)
