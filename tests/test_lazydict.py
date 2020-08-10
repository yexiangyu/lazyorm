import lazyorm.lazydict as ld


def none(val_type):
    return lambda x=None: val_type() if x is None else val_type(x)


class Human(ld.LazyDict):
    _keys = dict(
        id=none(str),
        name=none(str),
        age=none(int)
    )


class Female(Human):
    _keys = dict(
        gender=lambda x=None: "female"
    )


class Male(Human):
    _keys = dict(
        gender=lambda x=None: "male"
    )


def test_dict():
    h = Human()
    m = Male()
    f = Female()
    assert hasattr(m, '_keys')
    assert hasattr(f, '_keys')
    assert hasattr(h, '_keys')
    assert set(h.keys()).issubset(f.keys())
