#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.spec.convert import (
    DatasetLoader,
    DatapackLoader,
    Label,
    convert_datapack,
    convert_dataset,
    postprocess_collect_schema,
)

from rcabench_platform.v1.utils.serde import save_json

from pathlib import Path
from typing import Any

import polars as pl


def convert_traces(src: Path):
    assert src.exists(), src

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

    return lf


def convert_metrics(src: Path):
    assert src.exists(), src

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
        pl.col("_split").list.get(0).alias("service_name"),
        pl.col("_split").list.get(1).alias("metric"),
    )

    lf = lf.drop("_split")

    return lf


class RcaevalDatapackLoader(DatapackLoader):
    def __init__(self, src_folder: Path, dataset: str, datapack: str, service: str) -> None:
        self._src_folder = src_folder
        self._dataset = dataset
        self._datapack = datapack
        self._service = service

    def name(self) -> str:
        return self._datapack

    def labels(self) -> list[Label]:
        return [Label(level="service", name=self._service)]

    def data(self) -> dict[str, Any]:
        return {
            "inject_time.txt": self._src_folder / "inject_time.txt",
            "traces.parquet": convert_traces(self._src_folder / "traces.csv"),
            "simple_metrics.parquet": convert_metrics(self._src_folder / "simple_metrics.csv"),
        }

    def postprocess(self, folder: Path, files: set[str]):
        schema = postprocess_collect_schema(folder, files)
        save_json(schema, path=folder / "schema.json")


class RcaevalDatasetLoader(DatasetLoader):
    def __init__(self, src_folder: Path, dataset: str):
        self._src_folder = src_folder
        self._dataset = dataset

        datapack_loaders = []

        for service_path in src_folder.iterdir():
            if not service_path.is_dir():
                continue

            for num_path in service_path.iterdir():
                if not num_path.is_dir():
                    continue

                service = service_path.name
                num = num_path.name
                datapack = f"{service}_{num}"

                if num == "multi-source-data":
                    continue

                loader = RcaevalDatapackLoader(
                    src_folder=num_path,
                    dataset=dataset,
                    datapack=datapack,
                    service=service.split("_")[0],
                )

                datapack_loaders.append(loader)

        self._datapack_loaders = datapack_loaders

    def name(self) -> str:
        return self._dataset

    def __len__(self) -> int:
        return len(self._datapack_loaders)

    def __getitem__(self, index: int) -> DatapackLoader:
        return self._datapack_loaders[index]


@app.command()
@timeit()
def run(skip: bool = True, parallel: int | None = 4):
    src_root = Path("data") / "RCAEval"

    rcaeval_datasets = ["RE2-TT", "RE2-OB"]  # TODO: support all datasets

    for dataset in rcaeval_datasets:
        loader = RcaevalDatasetLoader(
            src_folder=src_root / dataset,
            dataset="rcaeval_" + dataset.lower().replace("-", "_"),
        )
        convert_dataset(loader, skip=skip, parallel=parallel)


@app.command()
@timeit()
def local_test_1():
    loader = RcaevalDatapackLoader(
        src_folder=Path("data/RCAEval/RE2-TT/ts-auth-service_cpu/1"),
        dataset="rcaeval_re2_tt",
        datapack="ts-auth-service_cpu_1",
        service="ts-auth-service",
    )
    convert_datapack(
        loader,
        dst_folder=Path("temp/rcaeval_re2_tt/ts-auth-service_cpu_1"),
        skip=False,
    )


@app.command()
@timeit()
def local_test_2():
    loader = RcaevalDatapackLoader(
        src_folder=Path("data/RCAEval/RE2-OB/checkoutservice_cpu/1"),
        dataset="rcaeval_re2_ob",
        datapack="checkoutservice_cpu_1",
        service="checkoutservice",
    )
    convert_datapack(
        loader,
        dst_folder=Path("temp/rcaeval_re2_ob/checkoutservice_cpu_1"),
        skip=False,
    )


if __name__ == "__main__":
    app()
