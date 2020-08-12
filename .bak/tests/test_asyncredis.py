import asyncio
import os
from lazyorm.lazyredis import AsyncRedisNode


async def test_lazyredis():

    redis_node = os.environ['REDIS_NODE']
    host, port = redis_node.split(":")
    port = int(port)
    password = os.environ['REDIS_PASSWORD']
    N = AsyncRedisNode(host=host, hash='hash001', port=port, password=password)
    await N.put('test', 'message to test')
    v = await N.get('test')
    print(v)

if __name__ == "__main__":
    asyncio.run(test_lazyredis())
