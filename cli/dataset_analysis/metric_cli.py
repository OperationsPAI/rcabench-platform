#!/usr/bin/env -S uv run -s
# function: analyze the static distribution of datasets, including service names,
# log lines, entry traces, metric names, span names, trace length distribution, time slices, QPM, and total duration
import functools
import json
import os
from pathlib import Path
from typing import Any

import polars as pl
from rcabench.openapi import (
    InjectionsApi,
)

from rcabench_platform.v2.cli.main import app
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.datasets.rcabench import RCABenchAnalyzerLoader
from rcabench_platform.v2.datasets.rcaeval import RCAEvalAnalyzerLoader
from rcabench_platform.v2.logging import logger
from rcabench_platform.v2.metrics.metrics_calculator import DatasetMetricsCalculator
from rcabench_platform.v2.utils.fmap import fmap_processpool


def _calculate_trace_depths_vectorized(df: pl.DataFrame) -> list[int]:
    trace_depths = []

    df = df.with_columns(
        pl.when(pl.col("parent_span_id") == "").then(None).otherwise(pl.col("parent_span_id")).alias("parent_span_id")
    )

    for trace_group in df.group_by("trace_id", maintain_order=False):
        trace_data = trace_group[1]

        span_depths = _compute_span_depths(trace_data)
        max_depth = max(span_depths.values()) if span_depths else 1
        trace_depths.append(max_depth)

    return trace_depths


def _compute_span_depths(trace_df: pl.DataFrame) -> dict[str, int]:
    spans_data = {
        row["span_id"]: row["parent_span_id"]
        for row in trace_df.select(["span_id", "parent_span_id"]).iter_rows(named=True)
    }

    span_depths = {}

    root_spans = [span_id for span_id, parent_id in spans_data.items() if parent_id is None]

    queue = [(span_id, 1) for span_id in root_spans]  # (span_id, depth)
    processed = set()

    while queue:
        current_span, current_depth = queue.pop(0)

        if current_span in processed:
            continue

        processed.add(current_span)
        span_depths[current_span] = current_depth

        children = [
            span_id
            for span_id, parent_id in spans_data.items()
            if parent_id == current_span and span_id not in processed
        ]

        for child in children:
            queue.append((child, current_depth + 1))

    for span_id in spans_data:
        if span_id not in span_depths:
            span_depths[span_id] = 1

    return span_depths


def _process_single_datapack_metrics(dataset: str, datapack: str) -> tuple[str, dict[str, Any]]:
    if dataset == "rcabench":
        loader = RCABenchAnalyzerLoader(datapack)
    elif dataset.startswith("rcaeval"):
        loader = RCAEvalAnalyzerLoader(dataset, datapack)
    else:
        assert False, f"Unknown dataset: {dataset}"

    try:
        calculator = DatasetMetricsCalculator(loader)
        return datapack, calculator.calculate_and_report()

    except Exception as e:
        logger.error(f"Error processing datapack {datapack} in dataset {dataset}: {e}")
        return datapack, {}


@app.command()
def batch_metrics(dataset: str, online: bool):
    folder = Path("data/rcabench-platform-v2/data") / dataset
    if not folder.exists():
        logger.error(f"Error: Dataset {dataset} does not exist")
        return

    if dataset == "rcabench" and online:
        datapacks = []
        with RCABenchClient() as client:
            injection_api = InjectionsApi(client)
            res = injection_api.api_v2_injections_get(tags=["absolute_anomaly"], size=1000000, page=1)
            assert res.data is not None and res.data.items is not None, (
                "No injections found with absolute anomaly degree"
            )
            datapacks = [i.injection_name for i in res.data.items if i.injection_name is not None]
            logger.info(f"Total retrieved: {len(datapacks)} valid datapacks")
    else:
        datapacks = [f.name for f in folder.iterdir() if f.is_dir()]

    if not datapacks:
        logger.error(f"Error: No datapacks found in dataset {dataset}")
        return

    tasks = [functools.partial(_process_single_datapack_metrics, dataset, datapack) for datapack in datapacks]

    cpu = os.cpu_count()
    assert cpu is not None, "CPU count is not available"

    results_list = fmap_processpool(
        tasks,
        parallel=cpu // 4,
        cpu_limit_each=4,
        ignore_exceptions=False,
    )

    # Convert results list to dictionary
    all_results = {datapack: results for datapack, results in results_list}

    logger.info(f"✓ Completed processing all {len(datapacks)} datapacks")

    # Save batch results
    output_file = f"temp/{dataset}_batch_metrics.json"
    Path("temp").mkdir(exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=4, ensure_ascii=False, default=str)

    logger.info(f"\nBatch results saved to: {output_file}")


if __name__ == "__main__":
    app()
