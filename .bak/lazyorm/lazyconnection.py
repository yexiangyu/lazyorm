import logging
from .lazymqtt import AsyncMQTTNode
from .lazyelastic import AsyncElasticNode
from .lazyredis import AsyncRedisNode

LOG = logging.getLogger('lazy.conn')

CONNECTIONS = {}


def _get_connection(conn_type, **kw):
    if conn_type == 'mqtt':
        return AsyncMQTTNode(**kw)

    if conn_type == 'elastic':
        return AsyncElasticNode(**kw)

    if conn_type == 'redis':
        return AsyncRedisNode(**kw)

    return None


def get_connection(conn_name, conn_type, **kw):
    """ connection singleton for redis/es/redis

    Args:
        conn_name (str): connection name
        conn_type (str): connection type, supposed to be choosed from redis/elastic/redis
        kw (dict): args to create Node, if missed, a None will returned

    Returns:
        LazyNode or None : AsyncMQTTNode, AsyncElasticNode, AsyncRedisNode
    """

    # make sure conn_type is correct
    assert conn_type in ['mqtt', 'elastic', 'redis']

    LOG.debug('getting connection=%s', dict(conn_name=conn_name, conn_type=conn_type))

    global CONNECTIONS

    # create empty connections if missed
    if conn_name not in CONNECTIONS:
        CONNECTIONS[conn_name] = {}

    # create typed connection if missed
    if conn_type not in CONNECTIONS[conn_name]:
        CONNECTIONS[conn_name][conn_type] = _get_connection(conn_type, **kw)

    conn = CONNECTIONS[conn_name][conn_type]
    LOG.debug('got connection=%s: %s, with args=%s', dict(conn_name=conn_name, conn_type=conn_type), repr(conn), repr(kw))
    return conn
