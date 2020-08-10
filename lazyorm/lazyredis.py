import logging
import redis
from .lazynode import LazyNode

LOG = logging.getLogger("lazy.redis")


class LazyRedis(LazyNode):
    def __init__(self, host='localhost', port=6379, password=None):
        self.client = redis.Redis(host=host, port=port, password=password, decode_responses=True)

    def get(self, key):
        self.client.get(key)

    def put(self, key, value):
        pass

    def delete(self, key):
        pass
