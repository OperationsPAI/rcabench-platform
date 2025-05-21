from pathlib import Path
from pprint import pprint

import polars as pl

TEMP = Path("temp")
TEMP_SDG = TEMP / "sdg"

DATA_ROOT = Path("data") / "rcabench_platform_datasets"
META_ROOT = DATA_ROOT / "__meta__"


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


if __name__ == "__main__":
    pprint(
        {
            "TEMP": TEMP,
            "DATA_ROOT": DATA_ROOT,
            "META_ROOT": META_ROOT,
        }
    )
