import asyncio as aio

from elasticsearch._async import client
from .ldict import LDict
from .lid import gen_id


class _Config(LDict):
    _cols = dict(
        host=lambda x=None: "localhost" if x is None else str(x),
        port=lambda x=None: 0 if x is None else int(x),
        username=lambda x=None: None if x is None else str(x),
        password=lambda x=None: None if x is None else str(x)
    )

    @property
    def host_n_port(self):
        return "%s:%d" % (self.host, self.port)


class RedisConfig(_Config):
    _cols = dict(
        port=lambda x=None: 6379 if x is None else int(x),
    )


class MqttConfig(_Config):
    _cols = dict(
        port=lambda x=None: 1883 if x is None else int(x),
        topics=lambda x=None: [] if x is None or not isinstance(x, list) else [str(t) for t in x],
        client_id=lambda x=None: gen_id() if x is None else str(x),
        qos=lambda x=None: 0 if x is None or x < 0 or x > 2 else x
    )


class ElasticConfig(_Config):
    _cols = dict(
        port=lambda x=None: 9200 if x is None else int(x),
    )


ES_CONFIG = None
RD_CONFIG = None
MQ_CONFIG = None


def setup_elastic(
    host=None,
    port=None,
    username=None,
    password=None
):
    global ES_CONFIG

    if ES_CONFIG is None:
        if host:
            ES_CONFIG = ElasticConfig(
                host=host,
                port=port,
                username=username,
                password=password
            )

    return ES_CONFIG


def setup_redis(
    host=None,
    port=None,
    username=None,
    password=None
):
    global RD_CONFIG

    if RD_CONFIG is None:
        if host:
            RD_CONFIG = RedisConfig(
                host=host,
                port=port,
                username=username,
                password=password
            )

    return RD_CONFIG


def setup_mqtt(
    host=None,
    port=None,
    username=None,
    password=None,
    topics=None,
    client_id=None,
    qos=None
):
    global MQ_CONFIG

    if MQ_CONFIG is None:
        if host:
            MQ_CONFIG = MqttConfig(
                host=host,
                port=port,
                username=username,
                password=password,
                topics=topics,
                client_id=client_id,
                qos=qos
            )

    return MQ_CONFIG


def setup_connections(
    es_host=None,
    es_port=None,
    es_username=None,
    es_password=None,
    rd_host=None,
    rd_port=None,
    rd_username=None,
    rd_password=None,
    mq_host=None,
    mq_port=None,
    mq_username=None,
    mq_password=None,
    mq_topics=None,
    mq_client_id=None,
    mq_qos=None
):
    return \
        setup_elastic(es_host, es_port, es_username, es_password),\
        setup_redis(rd_host, rd_port, rd_username, rd_password, mq_topics),\
        setup_mqtt(mq_host, mq_port, mq_username, mq_password, mq_topics, mq_client_id, mq_qos)
