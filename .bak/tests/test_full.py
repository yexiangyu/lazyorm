import asyncio
import os

from lazyorm.lazyconnection import init_connection, get_connection
from lazyorm.lazymodel import LazyModel
from lazyorm.lazyid import lazyid

es_node = os.environ['ES_NODE']
es_username = os.environ['ES_USERNAME']
es_password = os.environ['ES_PASSWORD']

redis_node = os.environ['REDIS_NODE']
redis_password = os.environ['REDIS_PASSWORD']
redis_host, redis_port = redis_node.split(":")
redis_port = int(redis_port)

mqtt_host = "localhost"
mqtt_port = 1883


async def run_full():
    import logging
    LOG = logging.getLogger("lazy.test")
    init_connection(sync=False)
    # get_connection("Human", 'redis', hash='human', host=redis_host, port=redis_port, password=redis_password)
    get_connection("Human", 'mqtt', topic='/human', host=mqtt_host, port=mqtt_port)
    get_connection("Human", 'elastic', index='human', es_node=es_node, es_username=es_username, es_password=es_password)

    class Human(LazyModel):
        _keys = dict(
            id=lambda x=None: lazyid() if x is None else x,
            age=int,
            name=str
        )

    class Female(Human):
        _keys = dict(
            gender=lambda: "female"
        )

    h = Human(age=13, name="Johe")

    LOG.info("create %s", h)

    f = Female(age=24, name='Alice')
    LOG.info("create %s", f)

    await h.amq_put()
    LOG.info("mqtt done")


if __name__ == "__main__":
    asyncio.run(run_full(), debug=True)
