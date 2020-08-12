import logging

LOG = logging.getLogger('lazy.dict')


class LazyDictMeta(type):
    def __new__(cls, name, bases, attrs):
        if '_keys' in attrs:
            _keys = attrs['_keys']
            for b in (b for b in bases if hasattr(b, '_keys')):
                for k, vt in b._keys.items():
                    if k in _keys:
                        continue
                    _keys[k] = vt
            attrs['_keys'] = _keys
        return type.__new__(cls, name, bases, attrs)


class LazyDict(dict, metaclass=LazyDictMeta):
    _keys = {}

    def __init__(self, **kw):
        for k, vt in self._keys.items():
            v = kw.get(k, None)
            self[k] = vt() if v is None else vt(v)
