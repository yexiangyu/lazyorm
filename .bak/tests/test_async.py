import os
import asyncio
import elasticsearch as eslastic
import hbmqtt.client as mqtt
import aioredis as redis
import logging as log
import json


class Node(object):
    pass


class EsNode(Node):
    def __init__(self, node, username, password, index):
        self.node = node
        self.username = username
        self.password = password
        self.index = index
        self.loop = None
        self.cli = eslastic.AsyncElasticsearch(hosts=[self.node], http_auth=(username, password), loop=self.loop)

    async def put(self, data):
        await self.cli.index(self.index, body=data)
        log.info("es put data done, len(data)=%d", len(data))


class MqNode(Node):
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic
        self.cli = None
        self.loop = None
        self.async_connected = False

    async def _async_call(self):
        if self.async_connected:
            log.info("seems to connected")
            if self.loop and self.loop.is_running():
                log.info("loop is running, all good")
                return

        if self.loop is None or self.loop.is_closed():
            log.info("loop is None or loop no longer running")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.loop = asyncio.get_running_loop()
        self.cli = mqtt.MQTTClient()
        await self.cli.connect(f"mqtt://{self.host}:{self.port}")
        await self.cli.subscribe([(self.topic, 0)])
        self.async_connected = True
        log.info("connected")

    async def put(self, data):
        await self._async_call()
        if isinstance(data, str):
            data = data.encode()
        await self.cli.publish(self.topic, data)
        log.info("published")
        return "fuck"


class RdNode(Node):
    def __init__(self, host, port, password, hash):
        self.host = host
        self.port = port
        self.password = password
        self.hash = hash
        self.cli = None
        self.async_connected = False

    async def _async_call(self):
        if self.async_connected:
            return

        self.cli = await redis.create_redis(f"redis://{self.host}:{self.port}", password=self.password)
        self.async_connected = True
        log.info("redis connected")

    async def put(self, key, data):
        await self._async_call()
        await self.cli.hset(self.hash, key, data)
        log.info("redis hset done")


if __name__ == "__main__":
    debug = False
    if debug:
        log.basicConfig(level=log.DEBUG)
    else:
        log.basicConfig(level=log.INFO)

    es_node = os.environ['ES_NODE']
    es_username = os.environ['ES_USERNAME']
    es_password = os.environ['ES_PASSWORD']

    redis_node = os.environ['REDIS_NODE']
    redis_password = os.environ['REDIS_PASSWORD']
    redis_host, redis_port = redis_node.split(":")
    redis_port = int(redis_port)

    mqtt_host = "localhost"
    mqtt_port = 1883

    es = EsNode(es_node, es_username, es_password, "test-index")
    mq = MqNode(mqtt_host, mqtt_port, '/test')
    rd = RdNode(redis_host, redis_port, redis_password, 'test_hash')

    payload = dict(
        id=0,
        name="fuck"
    )

    payload = json.dumps(payload)

    ret = asyncio.run(mq.put(payload))
    ret = asyncio.run(rd.put('test', payload))
    ret = asyncio.run(rd.put('test', payload))

    async def test_all():

        print(await mq.put(payload))
        await es.put(payload)
        # await rd.put('test001', payload)
        # await asyncio.sleep(1)
        return

    asyncio.run(test_all(), debug=debug)
