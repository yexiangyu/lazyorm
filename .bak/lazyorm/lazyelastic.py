import json
from elasticsearch import Elasticsearch, AsyncElasticsearch
from .lazynode import LazyNode, AsyncLazyNode
from .lazyid import lazyid
import logging

LOG = logging.getLogger('lazy.es')


def simple_query(offset, page_size, **kw):

    if not kw:
        return {
            "query": {"match_all": {}},
            "from": offset,
            "size": page_size
        }

    must = [dict(term=dict(k=v)) for k, v in kw.items()]

    return {
        "query": {
            "bool": dict(must=must)
        },
        "from": offset,
        "size": page_size
    }


class ElasticNode(LazyNode):
    def __init__(self, index=None, es_node="localhost:9200", es_username=None, es_password=None):
        assert isinstance(index, str), index
        assert isinstance(es_node, str), es_node
        assert isinstance(es_username, str) or es_username is None, es_username
        assert isinstance(es_password, str) or es_password is None, es_password

        http_auth = (es_username, es_password) if (es_username is not None and es_password is not None) else None

        self.client = Elasticsearch(
            [es_node],
            http_auth=http_auth
        )
        self.index = index
        LOG.debug('connect %s with index=%s, %s, %s', es_node, index, es_username, es_password)

    def get(self, doc_id=None, **kw):

        if doc_id:
            try:
                ret = self.client.get(self.index, doc_id)
            except Exception as e:
                return None

            return ret['_source']

        ret = self.search(offset=0, page_size=2, **kw)

        ret_num = len(ret)

        if not ret_num:
            return None

        if ret_num != 1:
            return None

        return ret[0]

    def put(self, data, doc_id=None):
        assert isinstance(data, (str, bytes)), data
        doc_id = doc_id or lazyid()
        self.client.index(
            self.index,
            body=data,
            id=doc_id
        )
        return self

    def search(self, offset=0,  page_size=10, **kw):

        body = simple_query(offset, page_size, **kw)

        ret = self.client.search(
            index=self.index,
            body=json.dumps(body)
        )
        return [hit['_source'] for hit in ret['hits']['hits']]


class AsyncElasticNode(AsyncLazyNode):
    def __init__(self, index=None, es_node="localhost:9200", es_username=None, es_password=None):
        assert isinstance(index, str), index
        assert isinstance(es_node, str), es_node
        assert isinstance(es_username, str) or es_username is None, es_username
        assert isinstance(es_password, str) or es_password is None, es_password

        http_auth = (es_username, es_password) if (es_username is not None and es_password is not None) else None

        self.client = AsyncElasticsearch(
            [es_node],
            http_auth=http_auth
        )

        self.index = index
        LOG.debug('connect %s with index=%s, %s, %s', es_node, index, es_username, es_password)

    async def get(self, doc_id=None, **kw):

        if doc_id:
            try:
                ret = await self.client.get(self.index, doc_id)
            except Exception as e:
                return None

            return ret['_source']

        ret = await self.search(offset=0, page_size=2, **kw)

        ret_num = len(ret)

        if not ret_num:
            return None

        if ret_num != 1:
            return None

        return ret[0]

    async def put(self, data, doc_id=None):
        assert isinstance(data, (str, bytes)), data
        doc_id = doc_id or lazyid()
        await self.client.index(
            self.index,
            body=data,
            id=doc_id
        )
        return self

    async def search(self, offset=0,  page_size=10, **kw):

        body = simple_query(offset, page_size, **kw)

        ret = await self.client.search(
            index=self.index,
            body=json.dumps(body)
        )
        return [hit['_source'] for hit in ret['hits']['hits']]
