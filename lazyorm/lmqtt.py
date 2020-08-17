import json
import asyncio as aio
from .logger import getLogger
from asyncio.tasks import wait_for
from .connection import setup_mqtt
from .lloop import get_loop
from hbmqtt.client import MQTTClient


MQTT = None

LOG = getLogger('mq')


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
            LOG.debug("async init already")
            return

        LOG.debug("async init start")
        self.cli = MQTTClient(self.client_id, loop=self.loop)
        LOG.debug("async init create client")
        await self.cli.connect("mqtt://%s:%d" % (self.host, self.port))
        LOG.debug("async init connected")
        if self.topics:
            await self.cli.subscribe(
                [(topic, self.qos) for topic in self.topics]
            )
            LOG.debug("topics=%s subscribed", self.topics)
        else:
            LOG.debug("not topics available, this is a publisher")
        LOG.debug("async init sub done")
        self.async_initialized = True
        self.getter = self.loop.create_task(self._recieve())
        LOG.debug("async init done")

    async def _recieve(self):

        if self.topics:
            LOG.debug("starting recv msg")
        else:
            LOG.debug("no topic, quit getter")
            return

        while True:
            msg = await self.cli.deliver_message()
            LOG.debug("new message arrived, topic=%s", msg.topic)
            if msg.topic in self.queues:
                q = self.queues[msg.topic]
                payload = msg.publish_packet.payload.data.decode()
                await q.put(payload)

    async def put(self, topic, data):
        await self._async_init()
        if isinstance(data, str):
            data = data.encode()
        await self.cli.publish(topic, data, self.qos)
        LOG.debug("put topic=%s return=%s", topic, repr(data))

    async def get(self, topic, timeout=None):

        await self._async_init()

        try:
            ret = await aio.wait_for(self.queues[topic].get(), timeout=timeout, loop=self.loop)
        except aio.exceptions.TimeoutError:
            ret = None

        LOG.debug("get topic=%s return=%s", topic, repr(ret))

        return json.loads(ret) if ret else None


def connect_mqtt():
    global MQTT

    if MQTT is None:
        config = setup_mqtt()
        if config is not None:
            MQTT = AsyncMQTT(config.host, config.port, config.username, config.password, config.topics, config.client_id, config.qos)
            LOG.info("mqtt instance created config=%s, loop_id=%s", config.host_n_port, 'none' if MQTT.loop is None else id(MQTT.loop))

    return MQTT
