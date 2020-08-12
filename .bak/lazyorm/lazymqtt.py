import asyncio as aio
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
        self.cli = None
        self.qos = qos
        self.topic = topic
        self.loop = None
        self.queue = None
        self.async_connected = False
        self.recv_task = None

    async def _async_init(self):
        if self.loop is None:
            self.loop = aio.get_event_loop()
            self.async_connected = False

        if not self.loop.is_running():
            self.loop = aio.get_event_loop()
            self.async_connected = False

        if self.async_connected:
            return

        if self.recv_task is not None:
            assert isinstance(self.recv_task, aio.Task)
            self.recv_task.cancel()

        self.cli = amqtt.MQTTClient(client_id=self.client_id)
        self.queue = aio.Queue(1024)  # TODO: will 32 good enough?
        await self.cli.connect(f"mqtt://{self.host}:{self.port}")
        await self.cli.subscribe([(self.topic, self.qos)])
        self.async_connected = True
        self.recv_task = self.loop.create_task(self._recv_msg())

    async def _recv_msg(self):
        while True:

            msg = await self.cli.deliver_message()

            payload = msg.publish_packet.payload.data

            if self.queue.full():
                await self.queue.get()
            await self.queue.put(payload)

    async def get(self, timeout=None):
        assert isinstance(timeout, (float, int)) or timeout is None, timeout

        await self._async_init()

        try:
            ret = await aio.wait_for(self.queue.get(), timeout=timeout)
            return ret.decode('utf-8')
        except aio.exceptions.TimeoutError:
            return None

    async def put(self, data):

        await self._async_init()
        assert isinstance(data, (str, bytes)), data
        await self.cli.publish(self.topic, data, qos=self.qos)
        LOG.info("publish payload=%s topic=%s", data, self.topic)

    def sync_get(self, timeout=None):
        return aio.run(self.get())

    def sync_put(self, data):
        return aio.run(self.put(data))
