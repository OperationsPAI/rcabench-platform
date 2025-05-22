#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.spec.data import DATA_ROOT, dataset_index_path, dataset_label_path
from rcabench_platform.v1.utils.fmap import fmap_threadpool
from rcabench_platform.v1.utils.fs import running_mark
from rcabench_platform.v1.utils.serde import save_parquet

from pathlib import Path
import functools
import shutil

import polars as pl


@timeit()
def copyfile(src_folder: Path, dst_folder, name: str):
    shutil.copyfile(src_folder / name, dst_folder / name)


@timeit()
def convert_traces_csv(src: Path, dst: Path):
    assert src.exists()

    lf = pl.scan_csv(src, infer_schema_length=50000)

    lf = lf.select(
        pl.from_epoch("startTime", time_unit="us").dt.replace_time_zone("UTC").alias("time"),
        pl.col("traceID").alias("trace_id"),
        pl.col("spanID").alias("span_id"),
        pl.col("serviceName").alias("service_name"),
        pl.col("operationName").alias("span_name"),
        pl.col("parentSpanID").alias("parent_span_id"),
        pl.col("duration").cast(pl.UInt64).mul(1000).alias("duration"),
    )

    lf.sink_parquet(dst)


@timeit()
def convert_metrics_csv(src: Path, dst: Path):
    assert src.exists()

    lf = pl.scan_csv(src, infer_schema_length=50000)

    lf = lf.with_columns(pl.from_epoch("time", time_unit="s").dt.replace_time_zone("UTC").alias("time"))

    lf = lf.unpivot(
        on=None,
        index="time",
        variable_name="metric",
        value_name="value",
    )

    lf = lf.with_columns(
        pl.col("metric").str.split("_").alias("_split"),
    )

    lf = lf.with_columns(
        pl.col("_split").list.get(0).alias("attr.service_name"),
        pl.col("_split").list.get(1).alias("metric"),
    )

    lf = lf.drop("_split")

    lf.sink_parquet(dst)


@timeit()
def convert_datapack(src_folder: Path, dst_folder: Path, dataset: str, datapack: str, *, skip: bool):
    needs_skip = skip and dst_folder.exists()

    if not needs_skip:
        with running_mark(dst_folder):
            copyfile(src_folder, dst_folder, "inject_time.txt")

            if dataset == "rcaeval_re2_tt":
                convert_traces_csv(src_folder / "traces.csv", dst_folder / "traces.parquet")
                convert_metrics_csv(src_folder / "simple_metrics.csv", dst_folder / "simple_metrics.parquet")
            else:
                raise NotImplementedError  # TODO

    index = {"dataset": dataset, "datapack": datapack}
    labels = [{**index, "gt.level": "service", "gt.name": datapack.split("_")[0]}]
    return index, labels


@timeit()
def convert_dataset(src_folder: Path, dataset: str, *, skip: bool):
    tasks = []
    for service_path in src_folder.iterdir():
        if not service_path.is_dir():
            continue
        for num_path in service_path.iterdir():
            if not num_path.is_dir():
                continue

            service = service_path.name
            num = num_path.name
            datapack = f"{service}_{num}"

            dst_folder = DATA_ROOT / dataset / datapack

            tasks.append(functools.partial(convert_datapack, num_path, dst_folder, dataset, datapack, skip=skip))

    results = fmap_threadpool(tasks, parallel=16)

    index_rows = []
    label_rows = []
    for result in results:
        index_rows.append(result[0])
        label_rows.extend(result[1])

    index_df = pl.DataFrame(index_rows).sort(by=pl.all())
    label_df = pl.DataFrame(label_rows).sort(by=pl.all())

    save_parquet(index_df, path=dataset_index_path(dataset))
    save_parquet(label_df, path=dataset_label_path(dataset))


@app.command()
@timeit()
def run(skip: bool = True):
    src_root = Path("data") / "RCAEval"

    rcaeval_datasets = ["RE2-TT"]  # TODO: support all datasets

    for dataset in rcaeval_datasets:
        convert_dataset(src_root / dataset, "rcaeval_" + dataset.lower().replace("-", "_"), skip=skip)


@app.command()
@timeit()
def local_test():
    convert_datapack(
        src_folder=Path("data/RCAEval/RE2-TT/ts-auth-service_cpu/1"),
        dst_folder=Path("temp/rcaeval_re2_tt/ts-auth-service_cpu_1"),
        dataset="rcaeval_re2_tt",
        datapack="ts-auth-service_cpu_1",
        skip=False,
    )


if __name__ == "__main__":
    app()
