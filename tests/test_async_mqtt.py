import time
import asyncio as aio
from lazyorm.lnode import init_mqtt
from lazyorm.lmodel import LModel


init_mqtt('AsyncModel', 'localhost', 1883, '/asyncmodel')


class AsyncModel(LModel):
    _async = True
    _cols = dict(
        id=str,
        name=str
    )


async def run_async_mqtt():
    am = AsyncModel(id="hello", name="world")

    assert am.id == 'hello'
    assert am.name == 'world'

    await am.mq_put()

    am2 = await AsyncModel.mq_get()

    assert am.id == am2.id
    assert am.name == am2.name


def test_async_mqtt():
    print("#########################:async")
    # loop.run_until_complete(run_async_mqtt())
    try:
        aio.run(run_async_mqtt())
    except Exception as e:
        print('exception')
    except BaseException as e:
        print('baseexception')


if __name__ == "__main__":
    test_async_mqtt()
