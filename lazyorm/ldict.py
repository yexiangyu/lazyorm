class LDictMeta(type):
    def __new__(cls, name, bases, attrs):
        for b in (b for b in bases if hasattr(b, '_cols')):
            for k, vt in b._cols.items():
                if k in attrs['_cols']:
                    continue
                attrs['_cols'][k] = vt

        return type.__new__(cls, name, bases, attrs)


class LDict(dict, metaclass=LDictMeta):
    _cols = {}

    def __init__(self, **kwargs):
        _kwargs = {}
        for k, vt in self._cols.items():
            v = kwargs.get(k, None)
            _kwargs[k] = vt() if v is None else vt(v)
        super().__init__(**_kwargs)

    def __getattr__(self, key):
        if key in self:
            return self[key]

        return self.__getattribute__(key)

    def __setattr__(self, key, value):
        if key not in self._cols:
            super().__setattr__(key, value)
        else:
            self[key] = self._cols[key](value)
