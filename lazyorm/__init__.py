import os
import logging

LOG = logging.getLogger("lazy")

CHANNEL = logging.StreamHandler()

FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

CHANNEL.setFormatter(FORMATTER)
LOG.addHandler(CHANNEL)

if 'LAZY_DEBUG' in os.environ:
    LOG.setLevel(logging.DEBUG)
else:
    LOG.setLevel(logging.INFO)

LOG.propagate = False
