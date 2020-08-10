import logging
from .lazymqtt import MQTTNode
from .lazyelastic import ElasticNode

LOG = logging.getLogger('lazy.conn')

CONNECTIONS = {}


def _get_connection(conn_type, **kw):
    if conn_type == 'mqtt':
        return MQTTNode(**kw)

    if conn_type == 'elastic':
        return ElasticNode(**kw)

    return None


def get_connection(conn_name, conn_type, **kw):

    assert conn_type in ['mqtt', 'elastic', 'redis']

    LOG.debug('getting connection=%s', dict(conn_name=conn_name, conn_type=conn_type))

    global CONNECTIONS

    if conn_name not in CONNECTIONS:
        CONNECTIONS[conn_name] = {}

    if conn_type not in CONNECTIONS[conn_name]:
        CONNECTIONS[conn_name][conn_type] = _get_connection(conn_type, **kw)

    conn = CONNECTIONS[conn_name][conn_type]
    LOG.debug('got connection=%s: %s, with args=%s', dict(conn_name=conn_name, conn_type=conn_type), repr(conn), repr(kw))
    return conn
