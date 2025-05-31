from ..spec.algorithm import Algorithm, AlgorithmArgs, AlgorithmAnswer
from ..logging import timeit, logger

from pathlib import Path
import random

import polars as pl


def find_service_names(dataset: str, input_folder: Path) -> list[str]:
    if dataset.startswith("rcaeval"):
        return rcaeval_load_service_names(input_folder)
    else:
        raise NotImplementedError


@timeit()
def rcaeval_load_service_names(input_folder: Path) -> list[str]:
    metrics = pl.scan_parquet(input_folder / "simple_metrics.parquet").select(pl.col("service_name")).unique()
    traces = pl.scan_parquet(input_folder / "traces.parquet").select(pl.col("service_name")).unique()
    metrics, traces = pl.collect_all([metrics, traces])

    metric_service_names = set(metrics["service_name"].to_list())
    trace_service_names = set(traces["service_name"].to_list())

    service_names = metric_service_names | trace_service_names
    return sorted(service_names)


class Random(Algorithm):
    def needs_cpu_count(self) -> int | None:
        return 4

    @timeit()
    def __call__(self, args: AlgorithmArgs) -> list[AlgorithmAnswer]:
        service_names = find_service_names(args.dataset, args.input_folder)

        logger.debug(f"found {len(service_names)} service names")

        random.shuffle(service_names)

        answers = [
            AlgorithmAnswer(level="service", name=name, rank=rank)  #
            for rank, name in enumerate(service_names, start=1)
        ]

        return answers
