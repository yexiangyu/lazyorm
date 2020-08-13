from helper import LOG, create_class_n_loop

_, Human = create_class_n_loop()


def test_ldict():
    h = Human()
    LOG.info("create %s", h)
    assert h.id
    assert h.age
    assert h.gender
    assert h.name
    assert h.address


if __name__ == "__main__":
    test_ldict()
