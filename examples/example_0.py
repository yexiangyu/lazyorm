import logging
import lazyorm as lorm

lorm.setDebugLevel(logging.DEBUG)


class Human(lorm.build_model(is_async=False)):
    _mq_topic = "human_topic"
    _es_index = "human_index"
    _rd_queue = "human_queue"
    _cols = dict(
        name=str,
        age=int
    )


if __name__ == "__main__":
    lorm.setup_mqtt('localhost')
    lorm.setup_redis('localhost')
    lorm.setup_elastic('localhost')

    john = Human(name="john", age=32)

    print(john)  # output: {'name': 'john', 'age': 32}

    # publish/sub john to mqtt
    john.mq_put()
    john_from_mq = Human.mq_get()

    assert john_from_mq == john

    # set john to redis as kv
    john.rd_set('john')
    john_from_rd = Human.rd_get('john')

    assert john_from_rd == john

    # put john as task to redis queue
    john.rd_rpush()
    john_from_rd_queue = Human.rd_lpop()
    assert john == john_from_rd_queue

    # set/get john to es:
    john.es_put('john')
    john_from_es = Human.es_get('john')
    assert john_from_es == john
