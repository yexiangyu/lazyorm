from elasticsearch import AsyncTransport
from elasticsearch import AsyncElasticsearch
import asyncio as aio
from .logger import getLogger
from .lloop import get_loop
from .connection import setup_elastic

LOG = getLogger("es")


class _AsyncTransport(AsyncTransport):
    def __init__(self, hosts, *args, loop=None, sniff_on_start=False, **kwargs):
        super().__init__(hosts, *args, sniff_on_start=False, **kwargs)
        self.loop = loop

    async def _async_init(self):

        if self.loop is None:
            self.loop = aio.get_running_loop()

        self.kwargs["loop"] = self.loop

        # Now that we have a loop we can create all our HTTP connections
        self.set_connections(self.hosts)
        self.seed_connections = list(self.connection_pool.connections[:])

        # ... and we can start sniffing in the background.
        if self.sniffing_task is None and self.sniff_on_start:
            self.last_sniff = self.loop.time()
            self.create_sniff_task(initial=True)


ELASTIC = None


def connect_elastic():

    global ELASTIC

    if ELASTIC is None:
        config = setup_elastic()
        if config is not None:
            if config.username is None or config.password is None:
                http_auth = None
            else:
                http_auth = (config.username, config.password)

            loop = get_loop()

            ELASTIC = AsyncElasticsearch([config.host_n_port], transport_class=_AsyncTransport, http_auth=http_auth, loop=loop)
            LOG.info("elastic instance created config=%s, loop_id=%s", config.host_n_port, 'none' if loop is None else id(loop))

    return ELASTIC
