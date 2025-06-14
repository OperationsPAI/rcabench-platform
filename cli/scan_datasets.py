#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.datasets.spec import get_datapack_folder, get_datapack_list, get_dataset_meta_file
from rcabench_platform.v2.utils.fmap import fmap_threadpool
from rcabench_platform.v2.utils.serde import save_parquet

from typing import Any
import functools

import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import polars as pl


@app.command()
@timeit()
def scan_rows_count(dataset: str):
    datapacks = get_datapack_list(dataset)

    tasks = [functools.partial(_scan_rows_count, dataset, datapack) for datapack in datapacks]
    results = fmap_threadpool(tasks, parallel=32)

    df = pl.DataFrame(results)
    save_parquet(df, path=get_dataset_meta_file(dataset, "rows_count.parquet"))


@timeit()
def _scan_rows_count(dataset: str, datapack: str) -> dict[str, Any]:
    datapack_folder = get_datapack_folder(dataset, datapack)

    lf_map: dict[str, pl.LazyFrame] = {}

    if dataset.startswith("rcaeval"):
        lf_map["traces"] = pl.scan_parquet(datapack_folder / "traces.parquet")
        lf_map["metrics"] = pl.scan_parquet(datapack_folder / "simple_metrics.parquet")
    elif dataset.startswith("rcabench"):
        normal_traces = pl.scan_parquet(datapack_folder / "normal_traces.parquet")
        abnormal_traces = pl.scan_parquet(datapack_folder / "abnormal_traces.parquet")

        normal_metrics = pl.scan_parquet(datapack_folder / "normal_metrics.parquet")
        abnormal_metrics = pl.scan_parquet(datapack_folder / "abnormal_metrics.parquet")

        normal_logs = pl.scan_parquet(datapack_folder / "normal_logs.parquet")
        abnormal_logs = pl.scan_parquet(datapack_folder / "abnormal_logs.parquet")

        lf_map["traces"] = pl.concat([normal_traces, abnormal_traces])
        lf_map["metrics"] = pl.concat([normal_metrics, abnormal_metrics])
        lf_map["logs"] = pl.concat([normal_logs, abnormal_logs])
    else:
        raise NotImplementedError

    ans = {
        "dataset": dataset,
        "datapack": datapack,
    }

    names = list(lf_map.keys())
    for name in names:
        lf = lf_map[name]
        del lf_map[name]

        count = lf.select(pl.len()).collect().item()
        ans[name + ".rows"] = count

    return ans


@app.command()
@timeit()
def plot_rows_count(show: bool = True):
    datasets = ["rcaeval_re2_tt", "rcabench_filtered"]

    df_list = []
    for dataset in datasets:
        df = pl.read_parquet(get_dataset_meta_file(dataset, "rows_count.parquet"))
        df_list.append(df)

    columns = ["traces.rows", "metrics.rows"]

    fig, axes = plt.subplots(1, len(columns), figsize=(20, 10))
    for i, column in enumerate(columns):
        ax: Axes = axes[i]
        for j, df in enumerate(df_list):
            ax.boxplot(df[column], positions=[j], widths=0.6, vert=True)
        ax.set_title(column)
        ax.set_xticks(range(len(df_list)))
        ax.set_xticklabels(datasets)
    plt.tight_layout()
    plt.savefig("temp/rows_count.png")

    if show:
        plt.show()


if __name__ == "__main__":
    app()
