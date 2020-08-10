from queue import Queue, Empty
import paho.mqtt.client as mqtt
from .lazynode import LazyNode
from .lazyid import lazyid
import logging
import functools
import time

LOG = logging.getLogger('lazy.mqtt')


class MQTTNode(LazyNode):
    def __init__(self, host="localhost", port=1883, client_id=None, topic=None, qos=0):

        client_id = client_id or "mqtt-dest-" + lazyid()

        self.host = host
        self.port = port
        self.client_id = client_id
        self.client = None
        self.qos = qos

        self.topic = None

        if topic is not None:
            assert topic.find("#") < 0, topic
            self.topic = topic

        self.queue = Queue()

        self.init()

    def init(self):
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
        time.sleep(0.01)  # add this sleep to block till loop fully started, or put/get might start even before loop started
        LOG.info("mqtt client=%s connected to %s:%d, topic=%s", self.client_id, self.host, self.port, repr(self.topic))

    def _args(self, **kw):
        payload = kw.get('payload', '')
        qos = kw.get('qos', self.qos)
        retain = kw.get('retain', False)
        return payload, qos, retain

    def get(self, **kw):

        _block = kw.get('block', True)
        _timeout = kw.get('timeout', None)

        start = time.time()

        while True:
            try:
                ret = self.queue.get(block=False)
                return ret
            except Empty:
                if not _block:
                    return None

                if _timeout is not None:
                    delta = time.time() - start
                    if delta > _timeout:
                        return None

                time.sleep(0.1)

    def put(self, **kw):
        payload, qos, retain = self._args(**kw)
        self.client.publish(self.topic, payload=payload, qos=qos, retain=retain)
        LOG.debug("publish payload=%s topic=%s", payload, repr(self.topic))

    def __del__(self):
        if self.client is not None:
            self.client.loop_stop()


if __name__ == "__main__":
    import time
    import threading

    a = MQTTNode(topic="/test")
    threading.Thread(target=lambda: print(a.get())).start()
    a.put(topic="/test", payload='test')
