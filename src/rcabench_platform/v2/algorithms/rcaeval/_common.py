from ...algorithms.spec import AlgorithmArgs, AlgorithmAnswer
from ...logging import timeit

from ...graphs.sdg.build_.rcaeval import load_inject_time as rcaeval_load_inject_time
from ...graphs.sdg.build_.rcabench import load_inject_time as rcabench_load_inject_time

from collections.abc import Callable
from pathlib import Path
from typing import Any


import polars as pl
import pandas as pd


class SimpleMetricsAdapter:
    def __init__(self, func: Callable[..., Any]) -> None:
        self.func = func

    def __call__(self, args: AlgorithmArgs) -> list[AlgorithmAnswer]:
        assert args.dataset == "rcaeval_re2_tt" or args.dataset.startswith("rcabench")

        inject_time = load_inject_time(args.dataset, args.input_folder)
        df = load_simple_metrics(args.dataset, args.input_folder)

        output = (self.func)(data=df, inject_time=inject_time, dataset="train-ticket")
        ranks: list[str] = output["ranks"]

        answers: list[AlgorithmAnswer] = []
        for rank, node_name in enumerate(ranks, start=1):
            service, _ = node_name.split("_", maxsplit=1)
            answers.append(AlgorithmAnswer(level="service", name=service, rank=rank))

        return answers


@timeit()
def load_inject_time(dataset: str, input_folder: Path) -> int:
    if dataset.startswith("rcaeval"):
        inject_time = rcaeval_load_inject_time(input_folder)
    elif dataset.startswith("rcabench"):
        inject_time = rcabench_load_inject_time(input_folder)
    else:
        raise NotImplementedError

    return int(inject_time.timestamp() * 1e9)


@timeit()
def load_simple_metrics(dataset: str, input_folder: Path) -> pd.DataFrame:
    if dataset.startswith("rcaeval"):
        lf = pl.scan_parquet(input_folder / "simple_metrics.parquet")

        df = convert_simple_metrics(lf)
        return df.to_pandas()

    if dataset.startswith("rcabench"):
        normal_lf = pl.scan_parquet(input_folder / "normal_metrics.parquet")
        abnormal_lf = pl.scan_parquet(input_folder / "abnormal_metrics.parquet")
        lf = pl.concat([normal_lf, abnormal_lf])

        lf = lf.with_columns(
            pl.coalesce(
                [
                    "attr.k8s.container.name",
                    "attr.k8s.deployment.name",
                    "attr.k8s.statefulset.name",
                ]
            ).alias("service_name")
        )

        df = convert_simple_metrics(lf)

        return df.to_pandas()

    raise NotImplementedError


def convert_simple_metrics(lf: pl.LazyFrame) -> pl.DataFrame:
    lf = lf.drop_nulls(subset=["service_name"])

    lf = lf.with_columns(pl.col("time").dt.timestamp(time_unit="ns"))

    lf = lf.with_columns(pl.concat_str(pl.col("service_name"), pl.col("metric"), separator="_").alias("metric"))

    lf = lf.select("time", "metric", "value")

    df = lf.collect()

    df = df.pivot("metric", index="time", values="value", aggregate_function="mean")

    assert df.columns[0] == "time"
    df = df.select("time", *sorted(df.columns[1:]))

    df = df.fill_nan(value=None).fill_null(strategy="backward").fill_null(strategy="forward")

    return df
