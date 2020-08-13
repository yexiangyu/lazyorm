from helper import create_class_n_loop, LOG
import time

loop, Human = create_class_n_loop(False)

human = Human()


def test_es():
    ret = human.es_put(doc_id=human.id)
    LOG.info("%s added to es", ret)
    ret2 = Human.es_get(doc_id=ret.id)
    assert ret == ret2

    while True:
        ret3 = Human.es_get(id=ret2.id)  # this could fail due to latency of write in elasticsearch
        if ret3 is None:
            time.sleep(1)
        else:
            break

    # assert ret == ret3

    total, rets = Human.es_search(offset=10)
    total, rets = Human.es_search(offset=20)


if __name__ == "__main__":
    test_es()
    loop.run_until_complete(Human._es.cli.close())
    LOG.info("close loop, done")
