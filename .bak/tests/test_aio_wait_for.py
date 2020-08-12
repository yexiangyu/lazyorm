import asyncio as aio


async def return_somethign():
    await aio.sleep(1)
    return 42


async def get_something():
    ret = await aio.wait_for(return_somethign(), timeout=0.2)
    print(ret)

aio.run(get_something())
