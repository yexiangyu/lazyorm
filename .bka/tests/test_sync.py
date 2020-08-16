import random
from helper import create_class_n_loop, LOG
import time

loop, Human = create_class_n_loop(False)

MAX_SIZE = 20

humans = [Human() for i in range(MAX_SIZE)]


def test_redis():
    # clean

    for h in humans:
        h.rd_set(h.id)
        hh = h.rd_get(h.id)
        assert hh == h
        ret = h.rd_del(h.id)

    LOG.info("redis set/get/del done")

    for h in humans:
        h.rd_hset(h.id)
        hh = h.rd_hget(h.id)
        assert hh == h
        ret = h.rd_hdel(h.id)

    LOG.info("redis hset/hget/hdel done")

    for h in humans:
        h.rd_rpush()

    while True:
        hh = Human.rd_lpop(block=False)
        if hh is None:
            break

    LOG.info("redis lpop/rpush done")

    # loop.run_until_complete(Human._rd.cli.delete('human_record'))
    # loop.run_until_complete(Human._rd.cli.delete('/human_record'))


def test_mqtt():
    for h in humans:
        h.mq_put()

    LOG.info("mqtt put %d human done", MAX_SIZE)

    humans_in_mq = []

    while False:
        h = Human.mq_get(timeout=0.5)
        if h is None:
            LOG.info('no more msg to get')
            break
        humans_in_mq.append(h)

    LOG.info("mqtt got %d msg", len(humans_in_mq))


def test_es():

    # clean
    loop.run_until_complete(Human._es.cli.indices.delete(index=Human._es.index, ignore=[400, 404]))  # clean index

    LOG.info("clean index")

    for h in humans:
        h.es_put(doc_id=h.id)

    LOG.info("put es done")

    h = Human.es_get(doc_id='AAAAAAAAAAAAAAAAA')

    assert h is None

    h = Human.es_get(doc_id=random.choice(humans).id)

    LOG.info("get by doc_id=%s, return %s", h.id, h)

    total, humans_in_es = None, None

    # wait and search
    if True:
        start = time.time()
        while True:
            total, humans_in_es = Human.es_search(page_size=MAX_SIZE)
            LOG.info("found %d total results, return %d results", total, len(humans_in_es))
            if total == MAX_SIZE:
                break
            time.sleep(1)

        LOG.info("tooks %.f seconds to wait all %d results", time.time() - start, MAX_SIZE)

    # random search
    if True:
        h = random.choice(humans)
        total, rets = Human.es_search(id=h.id)
        LOG.info("found %s", h)

    # delete all male
    if True:
        Human.es_delete(doc_id=random.choice(humans).id)
        Human.es_delete(gender='male')


if __name__ == "__main__":
    test_mqtt()
    test_redis()
    test_es()
    loop.run_until_complete(Human._es.cli.close())
    Human._rd.cli.close()
    LOG.info("close loop, done")
