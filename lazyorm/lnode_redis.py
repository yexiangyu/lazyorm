from .lid import lid
from .llog import getLogger
import asyncio as aio

LOG = getLogger('lazy.node')


class RedisNode(object):
    pass


def init_redis(name, host, port, topic, client_id=None, loop=None):
    pass
