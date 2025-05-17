from loguru import logger as global_logger
from loguru._logger import Logger  # type:ignore


class GlobalLogger(Logger):
    def __init__(self) -> None:
        pass

    def __getattr__(self, name):
        return getattr(global_logger, name)


def get_real_logger():
    return global_logger


def set_real_logger(logger_):
    global global_logger
    global_logger = logger_


logger = GlobalLogger()
