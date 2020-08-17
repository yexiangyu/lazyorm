# lazyorm

key/value 对接 mqtt/elasticsearch/redis 的懒人包

## 安装

`pip install git+https://github.com/yexiangyu/lazyorm@master -i https://mirrors.aliyun.com/pypi/simple/`

## 使用

```python
import logging
import lazyorm as lorm

# 设置 debug 模式
lorm.setDebugLevel(logging.DEBUG)

# 定义模型
class Human(lorm.build_model(is_async=False)):
    _mq_topic = "human_topic" # 默认mqtt 话题
    _es_index = "human_index" #默认 es 索引
    _rd_queue = "human_queue" # 默认redis 队列
    _rd_hash = "human_hash" # 默认redis hash
    _cols = dict(
        name=str,
        age=int
    )


if __name__ == "__main__":

    # 配置 redis/mqtt/elastic 连接
    lorm.setup_mqtt('localhost')
    lorm.setup_redis('localhost')
    lorm.setup_elastic('localhost')

    # 生成一个实例
    john = Human(name="john", age=32)


    print(john)  # output: {'name': 'john', 'age': 32}

    # 写入/读取 mqtt
    # publish/sub john to mqtt
    john.mq_put()
    john_from_mq = Human.mq_get()

    assert john_from_mq == john

    # 写入/读取 redis kv
    # set john to redis as kv
    john.rd_set('john', expire=300) # expire为过期时间
    john_from_rd = Human.rd_get('john')

    assert john_from_rd == john

    # put john as task to redis queue
    john.rd_rpush() # 写入队列
    john_from_rd_queue = Human.rd_lpop() # 读取队列
    assert john == john_from_rd_queue

    # set/get john to es:
    john.es_put('john') # 写入 es
    john_from_es = Human.es_get('john') # 读取es
    assert john_from_es == john
```
