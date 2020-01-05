import logging
import os
import pickle

from functools import wraps


def persistent(path: str):
    def deco(foo):
        @wraps(foo)
        def inner(*args, **kwargs):
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    logging.info(f'Using {path} as data source. Delete this file to force data re-fetch.')
                    return pickle.load(f)
            data = foo(*args, **kwargs)
            with open(path, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            return data
        return inner
    return deco


def chunks(iterable, chunk_size):
    """Splits an iterable into chunks of size <chunk_size>.

    The last chunk can be shorter due to obvious reasons (if reminder is not 0).
    """
    return (
        iterable[(0 + i):(chunk_size + i)] 
        for i in range(0, len(iterable), chunk_size)
    )

