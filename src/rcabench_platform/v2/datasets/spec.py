from ..config import get_config
from ..logging import timeit

from dataclasses import dataclass
from pathlib import Path
import shutil

import polars as pl


@dataclass(kw_only=True, slots=True, frozen=True)
class Label:
    level: str
    name: str


def get_dataset_meta_folder(dataset: str) -> Path:
    config = get_config()
    return config.data / "meta" / dataset


def get_dataset_meta_file(dataset: str, filename: str) -> Path:
    return get_dataset_meta_folder(dataset) / filename


def get_dataset_index_path(dataset: str) -> Path:
    return get_dataset_meta_file(dataset, "index.parquet")


def get_dataset_labels_path(dataset: str) -> Path:
    return get_dataset_meta_file(dataset, "labels.parquet")


def get_dataset_folder(dataset: str) -> Path:
    config = get_config()
    return config.data / "data" / dataset


def get_datapack_folder(dataset: str, datapack: str) -> Path:
    return get_dataset_folder(dataset) / datapack


def get_dataset_list() -> list[str]:
    config = get_config()
    meta_folder = config.data / "meta"
    datasets = [d.name for d in meta_folder.iterdir() if d.is_dir()]
    return datasets


def get_datapack_list(dataset: str) -> list[str]:
    index_df = read_dataset_index(dataset)

    ans = []
    for row in index_df.iter_rows(named=True):
        assert row["dataset"] == dataset
        assert isinstance(row["datapack"], str)
        datapack = row["datapack"]
        ans.append(datapack)

    return ans


def get_datapack_labels(dataset: str, datapack: str) -> list[Label]:
    labels_path = get_dataset_labels_path(dataset)

    labels_df = (
        pl.scan_parquet(labels_path)
        .filter(
            pl.col("dataset") == dataset,
            pl.col("datapack") == datapack,
        )
        .collect()
    )

    assert len(labels_df) >= 1, f"Labels for datapack `{datapack}` not found in dataset `{dataset}`"

    labels_set = set()
    labels_list = []
    for level, name in labels_df.select("gt.level", "gt.name").iter_rows():
        assert isinstance(level, str)
        assert isinstance(name, str)
        labels_set.add((level, name))
        labels_list.append(Label(level=level, name=name))

    assert len(labels_set) == len(labels_list), f"Duplicate labels found in `{dataset}/{datapack}`"

    return labels_list


def read_dataset_index(dataset: str) -> pl.DataFrame:
    index_path = get_dataset_index_path(dataset)
    index_df = pl.read_parquet(index_path)
    return index_df


def read_dataset_labels(dataset: str) -> pl.DataFrame:
    labels_path = get_dataset_labels_path(dataset)
    labels_df = pl.read_parquet(labels_path)
    return labels_df


@timeit()
def delete_dataset(dataset: str):
    meta_folder = get_dataset_meta_folder(dataset)
    data_folder = get_dataset_folder(dataset)

    shutil.rmtree(meta_folder, ignore_errors=True)
    shutil.rmtree(data_folder, ignore_errors=True)
