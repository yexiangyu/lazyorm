import asyncio
from lazyorm.lazymqtt import AsyncMQTTNode


async def test_lazymqtt():
    C = AsyncMQTTNode(topic='/test')
    await C.put(b'test')
    t = await C.get()
    print(t)


if __name__ == "__main__":
    asyncio.run(test_lazymqtt())
