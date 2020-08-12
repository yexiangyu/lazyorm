import time
import asyncio as aio
from lazyorm.lnode import init_mqtt
from lazyorm.lmodel import LModel

loop = aio.get_event_loop()
init_mqtt('SyncModel', 'localhost', 1883, '/syncmodel', loop=loop)


class SyncModel(LModel):
    _async = False
    _cols = dict(
        id=str,
        name=str
    )


def test_sync_mqtt():
    print("#########################:sync")
    sm = SyncModel(id="hello", name="world")
    assert sm.id == 'hello'
    assert sm.name == 'world'
    sm.mq_put()
    sm2 = SyncModel.mq_get()
    assert sm.id == sm2.id
    assert sm.name == sm2.name


if __name__ == "__main__":
    test_sync_mqtt()
