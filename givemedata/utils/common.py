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


