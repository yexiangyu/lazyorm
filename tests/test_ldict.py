from lazyorm.ldict import LDict

data = dict(id=1, name='test')


class Human(LDict):
    _cols = dict(
        id=str,
        name=str,
    )


def test_create():
    h = Human()
    assert h.id == ''
    assert h.name == ''
    h.id = 'test_id'
    assert h.id == 'test_id'


def test_inhere():
    class Male(Human):
        _cols = dict(
            gender=lambda x=None: 'male'
        )

    m = Male()

    assert m.gender == 'male'
    assert m.id == ''
    assert m.name == ''
