import logging


_LOG = logging.getLogger('lazy')
_LOG.setLevel(logging.INFO)

logging.basicConfig(level=logging.INFO)


def getLogger(name):
    if not name.startswith('lazy.'):
        name = 'lazy.' + name
    return logging.getLogger(name)
