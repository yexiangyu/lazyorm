from .lloop import get_loop
from .logger import getLogger
import asyncio as aio
import aioredis
from .connection import setup_redis

LOG = getLogger('rd')

REDIS = None


class AsyncRedis(object):
    def __init__(self, host, port, username, password, db):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db = db
        self.loop = get_loop()

        self.cli = None
        self.async_connected = False

    async def _async_init(self):

        if self.async_connected:
            return

        if self.loop is None:
            self.loop = aio.get_event_loop()

        self.cli = await aioredis.create_redis(
            f"redis://{self.host}:{self.port}",
            db=self.db,
            password=self.password,
            loop=self.loop
        )

        self.async_connected = True

    async def get(self, key):
        await self._async_init()
        ret = await self.cli.get(key)
        LOG.debug("get with key=%s, return %s", key, repr(ret))
        return ret.decode() if ret is not None else None

    async def set(self, key, value, **kw):
        await self._async_init()
        ret = await self.cli.set(key, value, **kw)
        LOG.debug("set with key=%s, kwargs=%s, return %s", key, kw,  repr(ret))
        return ret

    async def delete(self, key):
        await self._async_init()
        ret = await self.cli.delete(key)
        LOG.debug("del with key=%s, return %s", key, repr(ret))
        return ret

    async def hget(self, hash, key):
        await self._async_init()
        ret = await self.cli.hget(hash, key)
        LOG.debug("get with hash=%s, key=%s, return %s", hash, key, repr(ret))
        return ret.decode() if ret is not None else None

    async def hset(self, hash, key, value, **kwargs):
        await self._async_init()
        ret = await self.cli.hset(hash, key, value)
        LOG.debug("set with hash=%s, key=%s, return %s", hash, key, repr(ret))
        return ret

    async def hdelete(self, hash, key):
        await self._async_init()
        ret = await self.cli.hdel(hash, key)
        LOG.debug("del with hash=%s, key=%s, return %s", hash, key, repr(ret))
        return ret

    async def lpop(self, topic, timeout=0):
        await self._async_init()
        ret = await self.cli.blpop(topic, timeout=timeout)
        LOG.debug("lpop with topic=%s, timeout=%s, return %s", topic, repr(timeout),  repr(ret))
        return ret[1].decode() if ret is not None else None

    async def rpush(self, topic, data):
        await self._async_init()
        ret = await self.cli.rpush(topic, data)
        LOG.debug("rpush with topic=%s, return %s", topic, repr(ret))
        return ret


def connect_redis(*args, **kwargs):
    global REDIS

    if REDIS is None:
        config = setup_redis()
        if config is not None:
            REDIS = AsyncRedis(config.host, config.port, config.username, config.password, config.db)
            LOG.info("redis instance created config=%s, loop_id=%s", config.host_n_port, 'none' if REDIS.loop is None else id(REDIS.loop))

    return REDIS
