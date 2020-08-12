class lazy(dict):
    _keys = {}

    def __new__(cls, **kwargs):
        print(lazy is cls.mro()[0])
        return dict.__new__(cls, **kwargs)

    def __init__(self, **kwargs):
        print('test', kwargs)
        keys = list(kwargs.keys())
        for k in keys:
            if k not in lazy._keys:
                del(kwargs[k])
            else:
                kwargs[k] = _keys[k](kwargs[k])
        super().__init__(**kwargs)


l = lazy(a=1, b=2)

print(l)
