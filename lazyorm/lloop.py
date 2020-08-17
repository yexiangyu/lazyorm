from .logger import getLogger
import asyncio as aio

LOOP = None

LOG = getLogger('lo')


def get_loop():
    global LOOP
    if LOOP is None:
        try:
            LOOP = aio.get_running_loop()
            LOG.info("got runnging loop, loop_id=%s", id(LOOP))
        except RuntimeError:
            LOOP = aio.get_event_loop()
            LOG.info("create loop, loop_id=%s", id(LOOP))
    else:
        LOG.info("loop initialized already, loop_id=%s", id(LOOP))

    return LOOP
