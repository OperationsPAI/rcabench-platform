from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass(kw_only=True, frozen=True, slots=True)
class AlgorithmArgs:
    dataset: str
    datapack: str
    input_folder: Path
    output_folder: Path


@dataclass(kw_only=True, frozen=True, slots=True)
class AlgorithmAnswer:
    level: str
    name: str
    rank: int


class Algorithm(ABC):
    @abstractmethod
    def needs_cpu_count(self) -> int | None: ...

    @abstractmethod
    def __call__(self, args: AlgorithmArgs) -> list[AlgorithmAnswer]: ...


class AlgorithmRegistry(dict[str, Callable[[], Algorithm]]):
    pass


_GLOBAL_REGISTRY: AlgorithmRegistry = AlgorithmRegistry()


def global_algorithm_registry() -> AlgorithmRegistry:
    global _GLOBAL_REGISTRY
    return _GLOBAL_REGISTRY


def set_global_algorithm_registry(registry: AlgorithmRegistry):
    global _GLOBAL_REGISTRY
    _GLOBAL_REGISTRY = registry
