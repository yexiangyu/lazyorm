import logging
import os

LOG = logging.getLogger("lazy")

CHANNEL = logging.StreamHandler()

FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)16s - %(levelname)8s - %(message)s"
)

CHANNEL.setFormatter(FORMATTER)
LOG.addHandler(CHANNEL)

if 'LAZY_DEBUG' in os.environ:
    LOG.setLevel(logging.DEBUG)
else:
    LOG.setLevel(logging.INFO)

LOG.propagate = False


def getLogger(name):
    if not name.startswith('lazy.'):
        name = 'lazy.' + name
    return logging.getLogger(name)


def setDebugLevel(level):
    LOG.setLevel(level)
