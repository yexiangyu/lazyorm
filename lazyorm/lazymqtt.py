import asyncio
import hbmqtt.client as amqtt
from queue import Queue, Empty
import paho.mqtt.client as mqtt
from .lazynode import LazyNode, AsyncLazyNode
from .lazyid import lazyid
import logging
import functools
import time

LOG = logging.getLogger('lazy.mqtt')


class MQTTNode(LazyNode):
    def __init__(self, host="localhost", port=1883, client_id=None, topic=None, qos=0):
        assert isinstance(host, str), host
        assert isinstance(port, int), port

        client_id = client_id or "mqtt-dest-" + lazyid()

        assert isinstance(client_id, str)
        assert isinstance(topic, str)
        assert isinstance(qos, int)

        self.host = host
        self.port = port
        self.client_id = client_id
        self.client = None
        self.qos = qos
        self.topic = topic
        self.queue = Queue()

        self.initialize()

    def initialize(self):

        if self.client is None:
            self.client = mqtt.Client(client_id=self.client_id)
        else:
            self.client.reinitialise()

        def on_connect(client, userdata, flag, rc):
            self.client.subscribe(self.topic)
            LOG.info("%s connected and subscribe topic=%s", self.client_id, self.topic)

        def on_message(client, userdata, msg):
            LOG.debug("%s got message %s ", self.client_id, msg.payload)
            self.queue.put(msg.payload)

        self.client.on_connect = on_connect
        self.client.on_message = on_message

        self.client.connect(self.host, self.port)
        self.client.loop_start()
        time.sleep(0.5)  # add this sleep to block till loop fully started, or put/get might start even before loop started
        LOG.info("mqtt client=%s connected to %s:%d, topic=%s", self.client_id, self.host, self.port, repr(self.topic))

    def get(self, block=True, timeout=None):
        assert isinstance(timeout, (float, int)) or timeout is None, timeout
        start = time.time()
        while True:
            try:
                ret = self.queue.get(block=False)
                return ret
            except Empty:
                if not block:
                    return None

                if timeout is not None:
                    delta = time.time() - start
                    if delta > timeout:
                        return None

                time.sleep(0.1)

    def put(self, data):
        assert isinstance(data, (str, bytes)), data
        self.client.publish(self.topic, payload=data, qos=self.qos)
        LOG.debug("publish payload=%s topic=%s", data, self.topic)


class AsyncMQTTNode(AsyncLazyNode):
    def __init__(self, host="localhost", port=1883, client_id=None, topic=None, qos=0):
        assert isinstance(host, str), host
        assert isinstance(port, int), port

        client_id = client_id or "mqtt-dest-" + lazyid()

        assert isinstance(client_id, str)
        assert isinstance(topic, str)
        assert isinstance(qos, int)

        self.host = host
        self.port = port
        self.client_id = client_id
        self.client = None
        self.qos = qos
        self.topic = topic
        self.queue = asyncio.Queue()
        self.connected = False
        asyncio.ensure_future(self.initialize())

    async def initialize(self):

        self.client = amqtt.MQTTClient(client_id=self.client_id)
        await self.client.connect(f"mqtt://{self.host}:{self.port}")
        await self.client.subscribe([(self.topic, self.qos)])
        self.connected = True

        while True:
            msg = await self.client.deliver_message()
            pack = msg.publish_packet
            await self.queue.put(pack.payload.data)

    async def get(self, block=True, timeout=None):
        assert isinstance(timeout, (float, int)) or timeout is None, timeout
        start = time.time()
        while True:
            try:
                ret = await self.queue.get()
                return ret
            except Empty:
                if not block:
                    return None

                if timeout is not None:
                    delta = time.time() - start
                    if delta > timeout:
                        return None

                await asyncio.sleep(0.1)

    async def put(self, data):
        while not self.connected:
            LOG.info("mqtt not ready, sleep and retry")
            await asyncio.sleep(0.1)
        assert isinstance(data, (str, bytes)), data
        await self.client.publish(self.topic, data, qos=self.qos)
        LOG.info("publish payload=%s topic=%s", data, self.topic)
