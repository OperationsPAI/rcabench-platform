from .data import DATA_ROOT, dataset_index_path, dataset_label_path

from ..logging import timeit, logger
from ..utils.fs import running_mark
from ..utils.serde import save_csv, save_json, save_parquet, save_txt
from ..utils.fmap import fmap_processpool, fmap_threadpool

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import functools
import tempfile
import shutil
import sys

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

    @abstractmethod
    def postprocess(self, folder: Path, files: set[str]): ...


class DatasetLoader(ABC):
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def __getitem__(self, index: int) -> DatapackLoader: ...


def postprocess_collect_schema(folder: Path, files: set[str]) -> dict[str, Any]:
    ans = {}
    for file in files:
        if file.endswith(".parquet"):
            lf = pl.scan_parquet(folder / file)
            schema = lf.collect_schema()
            ans[file] = {k: str(v) for k, v in schema.items()}
    return ans


@timeit(log_args={"skip", "parallel"})
def convert_dataset(
    loader: DatasetLoader,
    *,
    skip: bool = True,
    parallel: int | None = None,
    ignore_exceptions: bool = False,
) -> None:
    dataset = loader.name()

    tasks = [functools.partial(_convert_datapack, loader, i, skip) for i in range(len(loader))]

    results = fmap_processpool(
        tasks,
        parallel=parallel,
        ignore_exceptions=ignore_exceptions,
    )

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


def _convert_datapack(loader: DatasetLoader, index: int, skip: bool) -> tuple[str, list[Label]]:
    datapack = loader[index]
    dst_folder = DATA_ROOT / loader.name() / datapack.name()
    return convert_datapack(datapack, dst_folder, skip=skip)


@timeit(log_args={"dst_folder", "skip"})
def convert_datapack(loader: DatapackLoader, dst_folder: Path, *, skip: bool = True) -> tuple[str, list[Label]]:
    needs_skip = skip and dst_folder.exists() and not (dst_folder / ".running").exists()

    if not needs_skip:
        with running_mark(dst_folder):
            with tempfile.TemporaryDirectory() as tempdir:
                tempdir = Path(tempdir)

                data = loader.data()
                keys = list(data.keys())

                for i, k in enumerate(keys, start=1):
                    save_data_file(tempdir, k, data[k])
                    del data[k]

                    size = (tempdir / k).stat().st_size
                    logger.debug(f"saved data [{i}/{len(keys)}] {loader.name()}/{k} size={human_byte_size(size)}")

                loader.postprocess(tempdir, set(keys))
                move_files(tempdir, dst_folder)

    datapack = loader.name()
    labels = loader.labels()
    return datapack, labels


@timeit(log_args={"src", "dst"})
def move_files(src: Path, dst: Path) -> None:
    for file in src.iterdir():
        shutil.move(file, dst / file.name)


def human_byte_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"

    s = float(size_bytes)
    for unit in ["KiB", "MiB"]:
        s /= 1024
        if s < 1024:
            return f"{s:.3f} {unit}"

    return f"{s:.3f} GiB"


@timeit(log_args={"dst_folder", "name"})
def save_data_file(dst_folder: Path, name: str, value: Any) -> None:
    file_path = dst_folder / name
    ext = file_path.suffix
    stem = file_path.stem

    if stem.endswith("traces"):
        validate_traces(value)
    elif stem.endswith("metrics"):
        validate_metrics(value)
    elif stem.endswith("logs"):
        validate_logs(value)

    sys.stdout.flush()

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
            "service_name": pl.String,
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
