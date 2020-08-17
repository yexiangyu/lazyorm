import sys

if not sys.version_info.major > 2:
    sys.exit("Sorry, Python <3 is not supported (yet)")

if not sys.version_info.minor >= 7:
    sys.exit("Sorry, Python <3.7 is not supported (yet)")

from .lloop import get_loop
from .lelastic import connect_elastic
from .lmqtt import connect_mqtt
from .lredis import connect_redis
from .connection import setup_connections
from .connection import setup_redis
from .connection import setup_mqtt
from .connection import setup_elastic
from .connection import MqttConfig
from .connection import ElasticConfig
from .connection import RedisConfig
from .lmodel import LObject, SLObject, build_model
from .ldict import LDict
from .logger import getLogger, setDebugLevel
from .lid import gen_id
