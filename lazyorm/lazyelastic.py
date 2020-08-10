import json
from elasticsearch import Elasticsearch
from .lazynode import LazyNode
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
    def __init__(self, index, es_node="localhost:9200", es_username=None, es_password=None):
        http_auth = (es_username, es_password) if (es_username is not None and es_password is not None) else None
        self.client = Elasticsearch(
            [es_node],
            http_auth=http_auth
        )
        self.index = index
        LOG.info('connect %s with index=%s, %s, %s', es_node, index, es_username, es_password)

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

    def put(self, doc_id=None, **kw):
        doc_id = doc_id or lazyid()
        self.client.index(
            self.index,
            body=json.dumps(kw),
            id=doc_id
        )
        LOG.info("put es with id=%s", doc_id)

    def search(self, offset=0,  page_size=10, **kw):

        body = simple_query(offset, page_size, **kw)

        ret = self.client.search(
            index=self.index,
            body=json.dumps(body)
        )

        return [hit['_source'] for hit in ret['hits']['hits']]
