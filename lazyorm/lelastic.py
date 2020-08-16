from elasticsearch import AsyncTransport
from elasticsearch import AsyncElasticsearch
import asyncio as aio
import json

import elasticsearch
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


class AsyncElastic(object):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.loop = get_loop()

        if self.username is None or self.password is None:
            http_auth = None
        else:
            http_auth = (self.username, self.password)

        self.cli = AsyncElasticsearch(
            ["%s:%d" % (self.host, self.port)],
            transport_class=_AsyncTransport,
            http_auth=http_auth
        )

    async def put(self, index, data, doc_id=None):
        await self.cli.index(index, data, id=doc_id)

    async def get(self, index, doc_id=None, **kwargs):
        if doc_id is not None:
            try:
                ret = await self.cli.get(index, id=doc_id)
                return ret['_source']
            except elasticsearch.exceptions.NotFoundError:
                return None

        _, rets = self.search(index, 0, 2, **kwargs)

        if len(rets) != 1:
            return None

        return rets[0]['_source']

    async def delete(self, index, doc_id=None, **kwargs):
        if doc_id is not None:
            return await self.cli.delete(index, id=doc_id)

        raise AttributeError()

    async def delete_by_query(self, index, query):
        raise AttributeError()

    async def search(self, index, offset=0, page_size=10, **kwargs):
        if not kwargs:
            query = \
                '''
                {
                    "from": %d,
                    "size": %d,
                    "query": {
                        "match_all": {}
                    }

                }
                ''' % (offset, page_size)
        else:
            query = \
                '''
                {
                    "from": %d,
                    "size": %d,
                    "query": {
                        "bool": {
                            "must": %s
                        }
                    }
                }
                ''' % (offset, page_size, json.dumps([dict(match={k: v}) for k, v in kwargs.items()]))

        try:
            total, rets = await self.search_by_query(index, query)
        except Exception as e:
            LOG.error(repr(e))
            total, rets = 0, []

        return total, rets

    async def search_by_query(self, index, query):
        rets = await self.cli.search(index=index, body=query)
        return rets['hits']['total']['value'], [hit['_source'] for hit in rets['hits']['hits']]


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

            ELASTIC = AsyncElastic(config.host, config.port, config.username, config.password)
            LOG.info("elastic instance created config=%s, loop_id=%s", config.host_n_port, 'none' if loop is None else id(loop))

    return ELASTIC
