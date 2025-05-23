from .data import DATA_ROOT, dataset_index_path, dataset_label_path

from ..logging import timeit
from ..utils.fs import running_mark
from ..utils.serde import save_csv, save_json, save_parquet, save_txt
from ..utils.fmap import fmap_threadpool

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import functools
import shutil

import polars as pl


@dataclass(kw_only=True, slots=True, frozen=True)
class Label:
    level: str
    name: str


class DatapackLoader(ABC):
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def labels(self) -> list[Label]: ...

    @abstractmethod
    def data(self) -> dict[str, Any]: ...


class DatasetLoader(ABC):
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def __getitem__(self, index: int) -> DatapackLoader: ...


@timeit(log_args={"skip", "parallel"})
def convert_dataset(loader: DatasetLoader, *, skip: bool = True, parallel: int | None = None) -> None:
    dataset = loader.name()

    tasks = []
    for i in range(len(loader)):
        datapack = loader[i]
        dst_folder = DATA_ROOT / dataset / datapack.name()
        tasks.append(functools.partial(convert_datapack, datapack, dst_folder, skip=skip))

    results = fmap_threadpool(tasks, parallel=parallel)

    index_rows = []
    label_rows = []
    for datapack, labels in results:
        index = {"dataset": dataset, "datapack": datapack}
        index_rows.append(index)
        for label in labels:
            label_rows.append({**index, "gt.level": label.level, "gt.name": label.name})

    index_df = pl.DataFrame(index_rows).sort(by=pl.all())
    label_df = pl.DataFrame(label_rows).sort(by=pl.all())

    save_parquet(index_df, path=dataset_index_path(dataset))
    save_parquet(label_df, path=dataset_label_path(dataset))


@timeit(log_args={"dst_folder", "skip"})
def convert_datapack(loader: DatapackLoader, dst_folder: Path, *, skip: bool = True) -> tuple[str, list[Label]]:
    needs_skip = skip and dst_folder.exists() and not (dst_folder / ".running").exists()

    if not needs_skip:
        with running_mark(dst_folder):
            data = loader.data()
            for k, v in data.items():
                save_data_file(dst_folder, k, v)

    datapack = loader.name()
    labels = loader.labels()
    return datapack, labels


def save_data_file(dst_folder: Path, name: str, value: Any):
    file_path = dst_folder / name
    ext = file_path.suffix
    stem = file_path.stem

    if stem.endswith("traces"):
        validate_traces(value)
    elif stem.endswith("metrics"):
        validate_metrics(value)
    elif stem.endswith("logs"):
        validate_logs(value)

    if isinstance(value, Path):
        assert value.exists()
        shutil.copyfile(value, file_path)

    elif ext == ".parquet":
        save_parquet(value, path=file_path)

    elif ext == ".csv":
        save_csv(value, path=file_path)

    elif ext == ".txt":
        save_txt(value, path=file_path)

    elif ext == ".json":
        save_json(value, path=file_path)

    else:
        raise NotImplementedError(f"Unsupported file type: {ext}")


def validate_traces(value: Any):
    if isinstance(value, (pl.LazyFrame, pl.DataFrame)):
        df = value

        required = {
            "time": pl.Datetime,
            "trace_id": pl.String,
            "span_id": pl.String,
            "parent_span_id": pl.String,
            "service_name": pl.String,
            "span_name": pl.String,
            "duration": pl.UInt64,
        }

        validate_by_model(df, required, extra_prefix="attr.")
    else:
        raise NotImplementedError  # TODO


def validate_metrics(value: Any):
    if isinstance(value, (pl.LazyFrame, pl.DataFrame)):
        df = value

        required = {
            "time": pl.Datetime,
            "metric": pl.String,
            "value": pl.Float64,
        }

        validate_by_model(df, required, extra_prefix="attr.")
    else:
        raise NotImplementedError  # TODO


def validate_logs(value: Any):
    if isinstance(value, (pl.LazyFrame, pl.DataFrame)):
        df = value

        required = {
            "time": pl.Datetime,
            "trace_id": pl.String,
            "span_id": pl.String,
            "service_name": pl.String,
            "level": pl.String,
            "message": pl.String,
        }

        validate_by_model(df, required, extra_prefix="attr.")

    else:
        raise NotImplementedError  # TODO


def validate_by_model(df: pl.LazyFrame | pl.DataFrame, model: dict[str, pl.DataType], extra_prefix: str):
    if isinstance(df, pl.LazyFrame):
        schema = df.collect_schema()
    elif isinstance(df, pl.DataFrame):
        schema = df.schema
    else:
        raise TypeError(f"Unsupported type: {type(df)}")

    for name, dtype in model.items():
        if name not in schema:
            raise ValueError(f"Missing required column: {name} {dtype}")
        if schema[name] != dtype:
            raise ValueError(f"Column {name} has incorrect type: {schema[name]}")

    for name, dtype in schema.items():
        if name not in model:
            if not name.startswith(extra_prefix):
                raise ValueError(f"Unexpected column: {name} {dtype}")
