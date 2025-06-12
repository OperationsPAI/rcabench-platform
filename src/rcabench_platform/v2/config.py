from dataclasses import dataclass
from pathlib import Path
from contextlib import contextmanager


@dataclass(kw_only=True)
class Config:
    data: Path
    output: Path
    temp: Path


_CONFIG = Config(
    data=Path("data/rcabench-platform-v2"),
    output=Path("output/rcabench-platform-v2"),
    temp=Path("temp"),
)


def get_config() -> Config:
    global _CONFIG
    return _CONFIG


def set_config(config: Config):
    global _CONFIG
    _CONFIG = config


@contextmanager
def current_config(config: Config):
    global _CONFIG

    old_config = _CONFIG
    _CONFIG = config

    try:
        yield
    finally:
        _CONFIG = old_config
