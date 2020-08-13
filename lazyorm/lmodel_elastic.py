import asyncio as aio
import json
from .ldict import LDict, LDictMeta
from .llog import getLogger
from .lnode_elastic import ESNode

LOG = getLogger('lazy.elastic')


async def _es_put(self, doc_id=None):
    assert self._es
    await self._es.put(json.dumps(self), doc_id=doc_id)
    return self


def _s_es_put(self, doc_id=None):
    return self._es.loop.run_until_complete(self._es_put(doc_id=doc_id))


@classmethod
async def _es_get(cls, doc_id=None, **kwargs):
    assert cls._es
    ret = await cls._es.get(doc_id=doc_id, **kwargs)
    if ret is None:
        return None
    return cls(**ret)


@classmethod
def _s_es_get(cls, doc_id=None, **kwargs):
    return cls._es.loop.run_until_complete(cls._es_get(doc_id=doc_id, **kwargs))


@classmethod
async def _es_search(cls, offset=0, page_size=10, **kwargs):
    assert cls._es
    ret = await cls._es.search(offset, page_size, **kwargs)
    return ret


@classmethod
def _s_es_search(cls, offset=0, page_size=10, **kwargs):
    return cls._es.loop.run_until_complete(cls._es_search(offset, page_size, **kwargs))


def meta_append_elastic_methods(name, attrs, is_async):

    assert isinstance(attrs, dict)
    assert isinstance(is_async, bool)

    if is_async:
        attrs['es_put'] = _es_put
        attrs['es_get'] = _es_get
        attrs['es_search'] = _es_search
    else:
        attrs['_es_put'] = _es_put
        attrs['_es_get'] = _es_get
        attrs['_es_search'] = _es_search
        attrs['es_put'] = _s_es_put
        attrs['es_get'] = _s_es_get
        attrs['es_search'] = _s_es_search

    _es = ESNode(name)

    if _es is None:
        if name != 'LModel':
            LOG.warning("model %s es not initialzed", name)
    else:
        if _es.loop is not None and is_async:
            LOG.warning("async model should not initialzed with external loop")
    attrs['_es'] = _es
