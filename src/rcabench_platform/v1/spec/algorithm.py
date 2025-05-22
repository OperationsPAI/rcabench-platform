from abc import ABC, abstractmethod
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
