import logging
import redis
import json
from .lazynode import LazyNode

LOG = logging.getLogger("lazy.redis")


class RedisNode(LazyNode):
    def __init__(self, host='localhost', hash=None, port=6379, password=None, db=0):
        assert isinstance(hash, (str, bytes)), hash
        self.client = redis.Redis(host=host, port=port, password=password, decode_responses=True, db=db)
        self.hash = hash
        LOG.info("connected %s:%d, hash=%s", host, port, hash)

    def get(self, key):
        return self.client.hget(self.hash, str(key))

    def put(self, key, data):
        assert isinstance(data, (str, bytes))
        self.client.hset(self.hash, key, data)

    def delte(self, *keys):
        assert len(keys), keys
        self.client.hdel(self.hash, *keys)
