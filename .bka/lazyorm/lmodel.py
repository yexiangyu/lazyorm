import asyncio as aio
import json
from .ldict import LDict, LDictMeta
from .llog import getLogger

from .lmodel_mqtt import meta_append_mqtt_methods
from .lmodel_elastic import meta_append_elastic_methods
from .lmodel_redis import meta_append_redis_methods

LOG = getLogger('lazy.model')


class LModelMeta(LDictMeta):
    def __new__(cls, name, bases, attrs):
        is_async = attrs.get('_async', None)

        if is_async is None:
            attrs['_async'] = True
            is_async = True

        for b in (b for b in bases if hasattr(b, '_async')):
            if b.__name__ == 'LModel':
                continue
            if b._async != is_async:
                if is_async:
                    raise ValueError("could not derive async model from sync model")
                else:
                    raise ValueError("could not derive sync model from async model")

        meta_append_mqtt_methods(name, attrs, is_async)
        meta_append_elastic_methods(name, attrs, is_async)
        meta_append_redis_methods(name, attrs, is_async)

        return LDictMeta.__new__(cls, name, bases, attrs)


class LModel(LDict, metaclass=LModelMeta):
    _cols = {}
    _async = True
    _mq = None
    _es = None
    _rd = None
