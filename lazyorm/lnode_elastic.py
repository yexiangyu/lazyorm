import json
from .llog import getLogger
import asyncio as aio
import elasticsearch as elastic

LOG = getLogger('lazy.elastic')


class ESTrans(elastic.AsyncTransport):
    def __init__(self, hosts, *args, loop=None, sniff_on_start=False, **kwargs):
        super().__init__(hosts, *args, sniff_on_start=False, **kwargs)
        self.loop = loop

    async def _async_init(self):
        """This is our stand-in for an async constructor. Everything
        that was deferred within __init__() should be done here now.

        This method will only be called once per AsyncTransport instance
        and is called from one of AsyncElasticsearch.__aenter__(),
        AsyncTransport.perform_request() or AsyncTransport.get_connection()
        """
        # Detect the async loop we're running in and set it
        # on all already created HTTP connections.
        # self.loop = get_running_loop()

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


def simple_search_query(offset, page_size, **kwargs):

    if not kwargs:
        return {
            "query": {"match_all": {}},
            "from": offset,
            "size": page_size
        }

    must = [dict(match={k: v}) for k, v in kwargs.items()]

    return {
        "query": {
            "bool": dict(must=must)
        },
        "from": offset,
        "size": page_size
    }


class _ESNode(object):
    def __init__(self, name, **kwargs):
        self.name = name
        self.host = kwargs.get('host', None)
        self.port = kwargs.get('port', None)
        self.index = kwargs.get('index', None)
        self.loop = kwargs.get('loop', None)
        self.username = kwargs.get("username", None)
        self.password = kwargs.get("password", None)

        self.cli = elastic.AsyncElasticsearch(
            [f"{self.host}:{self.port}"],
            transport_class=ESTrans,
            http_auth=None if (self.username is None or self.password is None) else (self.username, self.password)
        )
        LOG.info("es client %s initialzed, host=%s, port=%d, index=%s", self.name, self.host, self.port, self.index)

    async def put(self, data, doc_id=None):
        await self.cli.index(index=self.index, body=data, id=doc_id)
        LOG.debug("es put index=%s, id=%s done", self.index, repr(doc_id))

    async def get(self, doc_id=None, **kwargs):
        if doc_id is not None:
            try:
                ret = await self.cli.get(self.index, id=doc_id)
                LOG.debug("got id=%s from es", doc_id)
                return ret['_source']
            except elastic.NotFoundError as e:
                LOG.error("no such doc_id=%s", doc_id)
                return None

        _, rets = await self.search(offset=0, page_size=2, **kwargs)

        ret_num = len(rets)

        if ret_num != 1:
            LOG.warning("search return %d results", ret_num)
            return None

        return rets[0]

    async def search(self, offset=0, page_size=10, **kwargs):

        body = simple_search_query(offset, page_size, **kwargs)

        try:
            ret = await self.cli.search(
                index=self.index,
                body=json.dumps(body)
            )
        except Exception as e:
            LOG.error("search error: %s", repr(e))
            return 0, []

        total = ret['hits']['total']['value']

        hits = [hit['_source'] for hit in ret['hits']['hits']]

        LOG.debug("es return %d results in total=%d with offset=%d, page_size=%d, query=%s", len(hits), total, offset, page_size, body)

        return total, hits

    async def delete(self, doc_id=None, **kwargs):
        if doc_id is not None:
            try:
                ret = await self.cli.delete(self.index,  id=doc_id)
                LOG.debug("delete doc_id=%s: %s", doc_id, ret)
            except Exception as e:
                LOG.error("could not delete by doc_id=%s", doc_id)
            return

        if not kwargs:
            LOG.warning("no kwargs availale, ignore delete request")
            return

        query = {"query": {"match": kwargs}}

        try:
            ret = await self.cli.delete_by_query(self.index, body=json.dumps(query), ignore=[409])
            LOG.debug("delete by query=%s, ret=%s", query, ret)
        except elastic.ConflictError as e:
            LOG.error("failed to delete from es, query=%s: %s", query, repr(e))


class ESNode(object):
    _instances = {}

    def __new__(cls, name, **kwargs):
        if name not in cls._instances:
            if not kwargs:
                return None
            loop = kwargs.get('loop')
            LOG.debug("create es node instance %s: loop=%s", name, 'none' if loop is None else id(loop))
            cls._instances[name] = _ESNode(name, **kwargs)
            cls._instances[name].loop = loop

        inst = cls._instances[name]

        return inst


def init_elastic(name, host, port, index, username=None, password=None, loop=None):
    ESNode(name, host=host, port=port, index=index, username=username, password=password, loop=loop)
