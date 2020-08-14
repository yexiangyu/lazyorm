from setuptools import setup, find_packages

setup(
    name="lazyorm",
    version="0.3.8",
    keywords=("lazy", "elasticsearch", "mqtt", "redis", "orm"),
    long_description="lazy elastic/mqtt/redis orm, put/get/search/delete",
    license="MIT Licence",
    url="https://github.com/yexiangyu/lazyorm",
    author="yexiangyu",
    author_email="yexiangyu@maimenggroup.com",
    packages=['lazyorm'],
    platforms="any",
    install_requires=[
        'setuptools',
        'elasticsearch >=7.8.0',
        'aiohttp',
        'redis',
        'paho-mqtt',
        'nanoid',
        'hbmqtt',
        'Faker'
    ],
)
