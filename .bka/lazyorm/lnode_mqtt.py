from .lid import lid
from .llog import getLogger
import asyncio as aio
import hbmqtt.client as mqtt

LOG = getLogger('lazy.mqtt')


class _MQTTNode(object):
    def __init__(self, name, **kwargs):
        self.name = name
        self._kwargs = kwargs
        self.host = kwargs.get('host', None)
        self.port = kwargs.get('port', None)
        self.topic = kwargs.get('topic', None)
        self.qos = kwargs.get('qos', 0)
        self.client_id = kwargs.get('client_id', None)
        self.loop = kwargs.get('loop', None)

        assert isinstance(self.name, str), self.name

        assert self.host, self.host
        assert isinstance(self.host, str), self.host

        assert self.port, self.port
        self.port = int(self.port)

        assert self.topic, self.topic
        assert isinstance(self.topic, str), self.topic

        assert isinstance(self.qos, int), self.qos
        assert 0 <= self.qos <= 2

        if self.client_id is None:
            self.client_id = lid()
        assert isinstance(self.client_id, str), self.client_id
        self.queue = None
        self.cli = None
        self.recv_task = None
        self.async_connected = False

        LOG.info("mqtt client %s id=%s initialzed, host=%s, port=%d", self.name, self.client_id, self.host, self.port)

    async def _async_init(self):

        if self.async_connected:
            return

        if self.loop is None:
            self.loop = aio.get_event_loop()

        self.cli = mqtt.MQTTClient(self.client_id, loop=self.loop)

        LOG.debug("mqtt client internal loop=%d", id(self.cli._loop))
        await self.cli.connect(f"mqtt://{self.host}:{self.port}")
        LOG.debug("mqtt client connected with loop=%s in __async_init", id(self.loop))
        await self.cli.subscribe([(self.topic, self.qos)])
        self.queue = aio.Queue(1024, loop=self.loop)
        self.async_connected = True
        LOG.debug("mqtt client %s id=%s connected, host=%s, port=%d", self.name, self.client_id, self.host, self.port)

    async def put(self, data):
        await self._async_init()

        if isinstance(data, str):
            data = data.encode()

        await self.cli.publish(self.topic, data, qos=self.qos)

    async def get(self, timeout=None):
        await self._async_init()

        if self.recv_task is None:
            self.recv_task = self.loop.create_task(self._recv())

        try:
            ret = await aio.wait_for(self.queue.get(), timeout=timeout, loop=self.loop)
            LOG.debug("got mqtt msg=%s", ret)
            return ret
        except aio.exceptions.TimeoutError:
            LOG.debug("timeout=%s reached, return None", timeout)
            return None

    async def _recv(self):
        while True:

            msg = await self.cli.deliver_message()
            payload = msg.publish_packet.payload.data.decode()
            LOG.debug("recv mqtt msg in loop=%d, payload=%s", id(self.loop), payload)
            if self.queue.full():
                await self.queue.get()
            await self.queue.put(payload)
            LOG.debug("recv mqtt msg in loop=%d, payload=%s, queue put done", id(self.loop), payload)


class MQTTNode(object):
    _instances = {}

    def __new__(cls, name, **kwargs):
        if name not in cls._instances:
            if not kwargs:
                return None
            loop = kwargs.get('loop')
            LOG.debug("create mqtt node instance %s: loop=%s", name, 'none' if loop is None else id(loop))
            cls._instances[name] = _MQTTNode(name, **kwargs)

        inst = cls._instances[name]

        return inst


def init_mqtt(name, host, port, topic, client_id=None, loop=None):
    MQTTNode(name, host=host, port=port, topic=topic, client_id=client_id, loop=loop)
