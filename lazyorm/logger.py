import os
import logging


_LOG = logging.getLogger('lazy')
_LOG.setLevel(logging.INFO)


CHANNEL = logging.StreamHandler()

FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

CHANNEL.setFormatter(FORMATTER)
_LOG.addHandler(CHANNEL)

if 'LAZY_DEBUG' in os.environ:
    _LOG.setLevel(logging.DEBUG)
else:
    _LOG.setLevel(logging.INFO)

_LOG.propagate = False


def setDebugLevel(level):
    _LOG.setLevel(level)


def getLogger(name):
    if not name.startswith('lazy.'):
        name = 'lazy.' + name
    return logging.getLogger(name)
