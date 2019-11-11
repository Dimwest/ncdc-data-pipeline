import logging
from time import time
from typing import Callable

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create console handler and set level to info
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


def with_logging(fn: Callable) -> Callable:

    """
    Simple logging decorator

    :param fn: function to decorate
    :return:
    """

    def wrapper(*args, **kwargs):
        logger.info(f"Entering {fn.__name__} ...")
        ts = time()
        result = fn(*args, **kwargs)
        te = time()
        logger.info(f"Finished {fn.__name__} in {te-ts} seconds !")
        return result

    return wrapper
