#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.sources.convert import link_subset
from rcabench_platform.v2.utils.serde import save_parquet
from rcabench_platform.v2.datasets.spec import get_dataset_folder, get_dataset_meta_file, read_dataset_index
from rcabench_platform.v2.datasets.rcabench import FAULT_TYPES
from rcabench_platform.v2.clients.rcabench_ import RcabenchSdkHelper

from fractions import Fraction
import shutil

import polars as pl


@app.command()
@timeit()
def run(db_only: bool = False, require_filtered: bool = False):
    sdk = RcabenchSdkHelper()
    with_issues_resp = sdk.get_analysis_with_issues()

    rows = []
    for item in with_issues_resp:
        assert item.injection_name
        assert item.engine_config and item.engine_config.value
        row = {
            "injection_name": item.injection_name,
            "fault_type": FAULT_TYPES[item.engine_config.value],
        }
        rows.append(row)

    df = pl.DataFrame(rows)

    save_parquet(df, path=get_dataset_meta_file("rcabench", "with_issues.db.parquet"))

    if db_only:
        return

    full_df = read_dataset_index("rcabench").select("datapack").rename({"datapack": "injection_name"})
    df = df.join(full_df, on="injection_name", how="inner")

    if require_filtered:
        filtered_df = read_dataset_index("rcabench_filtered").select("datapack").rename({"datapack": "injection_name"})
        df = df.join(filtered_df, on="injection_name", how="inner")

    datapacks = df["injection_name"].to_list()

    dataset = "rcabench_with_issues"

    dataset_folder = get_dataset_folder(dataset)
    shutil.rmtree(dataset_folder, ignore_errors=True)

    link_subset(src_dataset="rcabench", dst_dataset=dataset, datapacks=datapacks)

    query_ratio()


@app.command()
@timeit()
def query_ratio():
    with_issues = read_dataset_index("rcabench_with_issues").select("datapack")
    filtered = read_dataset_index("rcabench_filtered").select("datapack")

    joint_df = with_issues.join(filtered, on="datapack", how="inner")

    ratio = Fraction(len(joint_df), len(with_issues))
    logger.info(f"rcabench_with_issues filtered ratio: {len(joint_df)}/{len(with_issues)} {float(ratio):.2%}")


if __name__ == "__main__":
    app()
