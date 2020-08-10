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

    # def __getattr__(self, key):
    #     if key in self._keys:
    #         return self[key]
    #     return getattr(super(), key)

    # def __setattr__(self, key, value):
    #     if key in self._keys:
    #         self[key] = self._keys[key](value)


if __name__ == "__main__":

    import uuid
    from enum import IntEnum
    import json
    import time

    class LogLevel(IntEnum):
        debug = 0
        info = 1
        warn = 2
        error = 3
        fatal = 4

    class GenericLog(LazyDict):
        _keys = dict(
            req_id=lambda x=None: str(uuid.uuid4()) if x is None else str(x),
            level=lambda x=None: LogLevel.info if x is None else LogLevel(x)
        )

    gl = GenericLog()

    print(json.dumps(gl))

    class AuthLog(GenericLog):
        _keys = dict(
            tenant_id=lambda x=None: str(uuid.uuid4()) if x is None else str(x),
            why=lambda x=None: 'don\'t know' if x is None else str(x),
            delta=float,
            time_tag=lambda x=None: time.time() if x is None else float(x)
        )

    al = AuthLog(delta=0.5)

    print(json.dumps(al))
