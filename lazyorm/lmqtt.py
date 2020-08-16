import asyncio as aio
from .logger import getLogger
from asyncio.tasks import wait_for
from .connection import setup_mqtt
from .lloop import get_loop
from hbmqtt.client import MQTTClient


MQTT = None

LOG = getLogger('mqtt')


class AsyncMQTT(object):
    def __init__(self, host, port, username, password, topics, client_id, qos):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.topics = topics
        self.loop = get_loop()
        self.queues = {}
        self.client_id = client_id
        self.qos = qos

        for topic in self.topics:
            self.queues[topic] = aio.Queue(loop=self.loop)

        self.async_initialized = False
        self.cli = None
        self.getter = None

    async def _async_init(self):

        if self.async_initialized:
            return

        self.cli = MQTTClient(self.client_id, loop=self.loop)
        await self.cli.connect("mqtt://%s:%d" % (self.host, self.port))
        await self.cli.subscribe(
            [(topic, self.qos) for topic in self.topics]
        )
        self.async_initialized = True
        self.getter = self.loop.create_task(self._recieve())

    async def _recieve(self):

        while True:
            msg = await self.cli.deliver_message()
            if msg.topic in self.queues:
                q = self.queues[msg.topic]
                payload = msg.publish_packet.payload.data.decode()
                await q.put(payload)

    async def put(self, topic, data):
        await self._async_init()
        await self.cli.publish(topic, data, self.qos)

    async def get(self, topic, timeout=None):

        await self._async_init()

        try:
            return await aio.wait_for(self.queues[topic].get(), timeout=timeout, loop=self.loop)
        except aio.exceptions.TimeoutError:
            return None


def connect_mqtt():
    global MQTT

    if MQTT is None:
        config = setup_mqtt()
        if config is not None:
            MQTT = AsyncMQTT(config.host, config.port, config.username, config.password, config.topics, config.client_id, config.qos)
            LOG.info("mqtt instance created config=%s, loop_id=%s", config.host_n_port, 'none' if MQTT.loop is None else id(MQTT.loop))

    return MQTT
