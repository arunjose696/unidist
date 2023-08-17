
from collections import defaultdict
import os
import time
import logging

def get_perf_logger(logger_name, rank, activate=True):
    logger = logging.getLogger(logger_name)
    if not logger.hasHandlers():
        sub_dir = f'.'
        # if not os.path.isdir(sub_dir):
        #     os.makedirs(sub_dir)
        file_name = f'{sub_dir}/worker_{rank}.log'
        f_format = logging.Formatter("%(message)s")
        f_handler = logging.FileHandler(file_name, delay=True)
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)
        old_debug = logger.debug
        def debug(*args, **kwargs):
            logger.disabled = False
            old_debug(*args, **kwargs)
        logger.debug = debug

    if activate:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.NOTSET)

    return logger


class PerfAutoLogger:
    sumup = defaultdict(int)
    level = 0
    timestamps = {}
    def __init__(self, name):
        self.name = name
        # PerfAutoLogger.timestamps[PerfAutoLogger.level] = time.perf_counter()
        logger_name = f'perf'
        self.logger = get_perf_logger(logger_name, os.getpid())

    def __enter__(self):
        prefix = '|\t'*PerfAutoLogger.level
        PerfAutoLogger.timestamps[PerfAutoLogger.level] = time.perf_counter()
        self.logger.debug(prefix + str(PerfAutoLogger.timestamps[PerfAutoLogger.level]) + '\t' + self.name)
        PerfAutoLogger.level += 1

    def __exit__(self, *args):
        PerfAutoLogger.level -= 1
        prefix = '|\t'*PerfAutoLogger.level
        exit_time = time.perf_counter()
        timediff = exit_time - PerfAutoLogger.timestamps[PerfAutoLogger.level]
        PerfAutoLogger.sumup[self.name] += timediff
        self.logger.debug(prefix + str(exit_time) + '\t' + f'({timediff})')
        del PerfAutoLogger.timestamps[PerfAutoLogger.level]
    
    def sumup_sum(x, y):
        for k, v in y.items():
            x[k] += v

        keys = list(x.keys())

        for k in keys:
            if 'Operation' in k:
                del x[k]
        
        return x


def perf_auto_logger(func):
    def decorator(*args, **kwargs):
        with PerfAutoLogger(func.__name__):
            return func(*args, **kwargs)

    return decorator
