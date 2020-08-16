class LDictMetaClass(type):
    def __new__(cls, name, bases, attrs):
        for base in bases:
            if getattr(base, '_cols', None) is not None:
                for k, vt in base._cols.items():
                    if k not in attrs['_cols']:
                        attrs['_cols'][k] = vt
        return type.__new__(cls, name, bases, attrs)


class LDict(dict, metaclass=LDictMetaClass):
    _cols = {}

    def __init__(self, **kwargs):
        for k, vt in self._cols.items():
            v = kwargs.get(k, None)
            self[k] = vt() if v is None else vt(v)

    def __getattr__(self, k):
        if k in self._cols:
            return self[k]

        return self.__getattribute__(k)

    def __setattr__(self, k, v):

        if k not in self._cols:
            super().__setattr__(k, v)
            return

        self[k] = self._cols[k](v)
