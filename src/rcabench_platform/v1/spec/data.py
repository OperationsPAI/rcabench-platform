from pathlib import Path
from pprint import pprint

import polars as pl

TEMP = Path("temp")
TEMP_SDG = TEMP / "sdg"

DATA_ROOT = Path("data") / "rcabench_platform_datasets"
META_ROOT = DATA_ROOT / "__meta__"

OUTPUT = Path("output")
OUTPUT_META = OUTPUT / "__meta__"


def dataset_index_path(dataset: str) -> Path:
    return META_ROOT / dataset / "index.parquet"


def dataset_label_path(dataset: str) -> Path:
    return META_ROOT / dataset / "label.parquet"


def get_datapack_list(dataset: str) -> list[tuple[str, Path]]:
    index_path = dataset_index_path(dataset)
    index_df = pl.read_parquet(index_path)

    ans = []

    for row in index_df.iter_rows(named=True):
        assert dataset == row["dataset"]
        assert isinstance(row["datapack"], str)

        datapack = row["datapack"]
        datapack_folder = DATA_ROOT / dataset / datapack

        ans.append((datapack, datapack_folder))

    return ans


def find_ground_truths(dataset: str, datapack: str) -> set[tuple[str, str]]:
    label_path = dataset_label_path(dataset)

    label_df = (
        pl.scan_parquet(label_path)
        .filter(
            pl.col("dataset") == dataset,
            pl.col("datapack") == datapack,
        )
        .collect()
    )

    assert len(label_df) >= 1, f"label of datapack `{datapack}` is not found in dataset `{dataset}`"

    ground_truths = set()
    for level, name in label_df[["gt.level", "gt.name"]].iter_rows():
        assert isinstance(level, str)
        assert isinstance(name, str)
        ground_truths.add((level, name))

    assert len(ground_truths) == len(label_df)

    return ground_truths


if __name__ == "__main__":
    pprint(
        {
            "TEMP": TEMP,
            "DATA_ROOT": DATA_ROOT,
            "META_ROOT": META_ROOT,
        }
    )
