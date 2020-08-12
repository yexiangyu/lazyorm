import asyncio
from lazyorm.lazymqtt import AsyncMQTTNode


async def test_lazymqtt():
    C = AsyncMQTTNode(topic='/test')
    await C.put(b'test')
    t = await C.get()
    print(t)


def test_synclazymqtt():
    C = AsyncMQTTNode(topic='/test')
    C.sync_put(b'test')
    ret = C.sync_get()
    print(ret)


if __name__ == "__main__":
    asyncio.run(test_lazymqtt())
    test_synclazymqtt()
