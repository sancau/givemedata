from functools import partial, wraps
from multiprocessing import cpu_count, Pool

import numpy as np
import pandas as pd


DEFAULT_WORKERS_COUNT = cpu_count()


def _wrapper(frame, job, axis):
    return frame.apply(job, axis=axis)


def multiprocess_apply(df, job, axis=1, workers=DEFAULT_WORKERS_COUNT):
    assert axis == 1, 'Only axis=1 is supported.'
    chunk_size = df.shape[0] // workers
    chunks = [chunk for _, chunk in df.groupby(np.arange(len(df)) // chunk_size)]
    if len(chunks) == workers + 1:
        last = chunks.pop()
        chunks[-1] = pd.concat([chunks[-1], last])
    pool = Pool(workers)
    return pd.concat(pool.map(partial(_wrapper, job=job, axis=axis), chunks))


def build_pipeline(operations):
    def pipeline(row):
        for operation in operations:
            row = operation(row)
        return row
    return pipeline


def apply_pipeline(df, operations, workers=None, axis=1):
    assert axis == 1, 'Only axis=1 is supported.'
    pipeline = build_pipeline(operations)
    if workers:
        return multiprocess_apply(df, pipeline, axis=axis, workers=workers)
    return df.apply(pipeline, axis=axis)


def return_empty_if_empty(foo):
    def deco(df):
        if df.empty:
            return df
        else:
            return foo(df)
    return deco
