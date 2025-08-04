#!/usr/bin/env -S uv run -s
# function: analyze the static distribution of datasets, including service names,
# log lines, entry traces, metric names, span names, trace length distribution, time slices, QPM, and total duration
import functools
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import matplotlib
import polars as pl

from rcabench_platform.v2.cli.main import app
from rcabench_platform.v2.datasets.spec import get_datapack_folder
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.logging import logger
from rcabench_platform.v2.metrics.dataset_loader import DatasetLoader
from rcabench_platform.v2.metrics.metrics_calculator import DatasetMetricsCalculator
from rcabench_platform.v2.utils.fmap import fmap_processpool

matplotlib.use("Agg")


def get_files(dataset: str) -> dict[str, list[str]]:
    if dataset.startswith("rcabench"):
        return {
            "trace": ["abnormal_traces.parquet", "normal_traces.parquet"],
            "logs": ["abnormal_logs.parquet", "normal_logs.parquet"],
            "metrics": [
                "abnormal_metrics.parquet",
                "normal_metrics.parquet",
                "abnormal_metrics_sum.parquet",
                "normal_metrics_sum.parquet",
                "abnormal_metrics_histogram.parquet",
                "normal_metrics_histogram.parquet",
            ],
        }

    if dataset.startswith("eadro"):
        return {
            "trace": ["trace.parquet"],
            "logs": ["log.parquet"],
            "metrics": ["metric.parquet"],
        }

    if dataset.startswith("rcaeval"):
        return {
            "trace": ["traces.parquet"],
            "metrics": ["simple_metrics.parquet"],
        }

    if dataset.startswith("aiops21"):
        return {
            "trace": ["traces.parquet"],
            "logs": ["logs.parquet"],
            "metrics": ["metrics.parquet"],
        }

    if dataset.startswith("nezha"):
        return {
            "trace": ["trace.parquet"],
            "logs": ["log.parquet"],
            "metrics": ["metric.parquet"],
        }

    raise ValueError(f"Unknown dataset: {dataset}")


@dataclass
class Metadata:
    ServiceNames: set[str] = field(default_factory=set)
    LogLines: int = 0
    EntryTrace: int = 0
    MetricNames: set[str] = field(default_factory=set)
    SpanNames: set[str] = field(default_factory=set)
    TraceLengthDistribution: dict[str, int] = field(default_factory=dict)
    TimeSlices: list[tuple[Any, Any]] = field(default_factory=list)
    QPM: float = 0.0
    TotalDurationSeconds: float = 0.0  # New field

    def to_dict(self):
        return {
            "ServiceNames": sorted(list(self.ServiceNames)),
            "LogLines": self.LogLines,
            "EntryTrace": self.EntryTrace,
            "MetricNames": sorted(list(self.MetricNames)),
            "SpanNames": sorted(list(self.SpanNames)),
            "TraceLengthDistribution": self.TraceLengthDistribution,
            "TimeSlices": self.TimeSlices,
            "QPM": self.QPM,
            "TotalDurationSeconds": self.TotalDurationSeconds,  # New output
        }


def _scan_metric(file: Path) -> Metadata:
    # time, metric, value, service_name
    assert file.exists()
    df = pl.scan_parquet(file)

    metadata = Metadata()
    metadata.ServiceNames = set(df.select("service_name").unique().collect().to_series().to_list())
    metadata.MetricNames = set(df.select("metric").unique().collect().to_series().to_list())

    time_data = df.select("time").collect().to_series()
    if len(time_data) > 0:
        min_time = time_data.min()
        max_time = time_data.max()
        assert min_time is not None and max_time is not None, "Time data cannot be empty"
        metadata.TimeSlices = [(min_time, max_time)]

    return metadata


def _scan_log(file: Path) -> Metadata:
    assert file.exists()
    df = pl.scan_parquet(file)

    metadata = Metadata()
    metadata.ServiceNames = set(df.select("service_name").unique().collect().to_series().to_list())
    metadata.LogLines = df.select(pl.len()).collect().item()

    time_data = df.select("time").collect().to_series()
    if len(time_data) > 0:
        min_time = time_data.min()
        max_time = time_data.max()
        assert min_time is not None and max_time is not None, "Time data cannot be empty"
        metadata.TimeSlices = [(min_time, max_time)]

    return metadata


def _scan_trace(file: Path) -> Metadata:
    # time, trace_id, span_id, parent_span_id, service_name, span_name, duration
    assert file.exists()
    df = pl.scan_parquet(file)

    metadata = Metadata()

    aggregated_info = df.select(
        [
            pl.col("time").min().alias("min_time"),
            pl.col("time").max().alias("max_time"),
            pl.when((pl.col("parent_span_id") == "").or_(pl.col("parent_span_id").is_null()))
            .then(1)
            .otherwise(0)
            .sum()
            .alias("entry_trace_count"),
        ]
    ).collect()

    service_names = df.select(pl.col("service_name").unique()).collect().to_series().to_list()
    span_names = df.select(pl.col("span_name").unique()).collect().to_series().to_list()

    metadata.ServiceNames = set(service_names)
    metadata.SpanNames = set([extract_path(s) for s in span_names])
    metadata.EntryTrace = aggregated_info["entry_trace_count"][0]

    min_time = aggregated_info["min_time"][0]
    max_time = aggregated_info["max_time"][0]
    if min_time is not None and max_time is not None:
        metadata.TimeSlices = [(min_time, max_time)]

    trace_spans = df.select(["trace_id", "span_id", "parent_span_id"]).collect()

    depth_results = _calculate_trace_depths_vectorized(trace_spans)

    if depth_results:
        depth_df = pl.DataFrame({"depth": depth_results})
        depth_counts = depth_df.group_by("depth").agg(pl.len().alias("count")).sort("depth")

        metadata.TraceLengthDistribution = {
            str(row["depth"]): row["count"] for row in depth_counts.iter_rows(named=True)
        }

    return metadata


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


def merge_metadata(metadata_list: list[Metadata]) -> Metadata:
    merged = Metadata()
    all_time_slices = []

    for metadata in metadata_list:
        merged.ServiceNames.update(metadata.ServiceNames)
        merged.LogLines += metadata.LogLines
        merged.EntryTrace += metadata.EntryTrace
        merged.MetricNames.update(metadata.MetricNames)
        merged.SpanNames.update(metadata.SpanNames)

        for length, count in metadata.TraceLengthDistribution.items():
            merged.TraceLengthDistribution[length] = merged.TraceLengthDistribution.get(length, 0) + count

        all_time_slices.extend(metadata.TimeSlices)

    if all_time_slices:
        sorted_slices = sorted(all_time_slices, key=lambda x: x[0])
        merged_slices = []

        current_start, current_end = sorted_slices[0]

        for start, end in sorted_slices[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged_slices.append((current_start, current_end))
                current_start, current_end = start, end

        merged_slices.append((current_start, current_end))
        merged.TimeSlices = merged_slices

        total_time_seconds = 0
        for start, end in merged.TimeSlices:
            if hasattr(start, "timestamp"):
                duration_seconds = end.timestamp() - start.timestamp()
            else:
                duration_seconds = end - start
            total_time_seconds += duration_seconds

        merged.TotalDurationSeconds = total_time_seconds  # Assign value

        total_time_minutes = total_time_seconds / 60.0
        if total_time_minutes > 0:
            merged.QPM = merged.EntryTrace / total_time_minutes

    return merged


def process_datapack(datapack: Path, files):
    trace_tasks = [functools.partial(_scan_trace, datapack / f) for f in files["trace"]]

    if "logs" not in files or not files["logs"]:
        log_tasks = []
    else:
        log_tasks = [functools.partial(_scan_log, datapack / f) for f in files["logs"]]
    metric_tasks = [functools.partial(_scan_metric, datapack / f) for f in files["metrics"]]
    total_tasks = trace_tasks + log_tasks + metric_tasks
    return total_tasks


@app.command()
def distribution(dataset: str):
    files = get_files(dataset)
    folder = Path("data/rcabench-platform-v2/data") / dataset
    datapacks = [f for f in folder.iterdir() if f.is_dir()]

    tasks = []

    for datapack in datapacks:
        tasks.extend(process_datapack(datapack, files))

    os.environ["POLARS_MAX_THREADS"] = str(15)

    results = fmap_processpool(
        tasks,
        parallel=8,
    )

    metadata = merge_metadata(results)

    with open(f"temp/{dataset}_metadata.json", "w") as f:
        json.dump(metadata.to_dict(), f, indent=4, default=str)


@app.command()
def metrics(dataset: str, datapack: str):
    """
    Calculate 4 metrics for a specified datapack in the dataset:
    - SDD: Service Distance to root cause
    - AC: Anomaly Cardinality
    - CPL: Causal Path Length
    - Root Service Degree: Maximum degree of root cause services
    """
    # Validate if datapack exists
    datapack_folder = get_datapack_folder(dataset, datapack)
    if not datapack_folder.exists():
        logger.error(f"Error: Datapack {datapack} does not exist in dataset {dataset}")
        return

    # Initialize DatasetLoader and MetricsCalculator
    try:
        loader = DatasetLoader(dataset, datapack)
        calculator = DatasetMetricsCalculator(loader)

        results = {}

        # 1. Service Distance to root cause (SDD@1, SDD@3, SDD@5)
        sdd_1 = calculator.compute_sdd(k=1)
        sdd_3 = calculator.compute_sdd(k=3)
        sdd_5 = calculator.compute_sdd(k=5)

        results["SDD@1"] = sdd_1
        results["SDD@3"] = sdd_3[:3] if isinstance(sdd_3, list) else [sdd_3]
        results["SDD@5"] = sdd_5[:5] if isinstance(sdd_5, list) else [sdd_5]

        # 2. Anomaly Cardinality (AC) - all services
        ac_results = calculator.compute_ac()
        results["AC"] = ac_results

        # 3. Causal Path Length (CPL)
        cpl = calculator.compute_cpl()
        results["CPL"] = cpl

        # 4. Root Service Degree
        root_service_degree = calculator.get_root_service_degree()
        results["RootServiceDegree"] = root_service_degree

        # Output results
        logger.info(f"\n=== Metrics Calculation Results for Dataset {dataset} - Datapack {datapack} ===")
        logger.info(f"SDD@1 (Service Distance to root cause): {sdd_1}")
        logger.info(f"SDD@3: {results['SDD@3']}")
        logger.info(f"SDD@5: {results['SDD@5']}")
        logger.info(f"CPL (Causal Path Length): {cpl}")
        logger.info(f"Root Service Degree: {root_service_degree}")
        logger.info("AC (Anomaly Cardinality) per service:")
        for service, count in ac_results.items():
            logger.info(f"  {service}: {count}")

        # Save results to file
        output_file = f"temp/{dataset}_{datapack}_metrics.json"
        Path("temp").mkdir(exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False, default=str)

        logger.info(f"\nResults saved to: {output_file}")

    except Exception as e:
        logger.error(f"Error occurred while calculating metrics: {str(e)}")
        import traceback

        traceback.print_exc()


def _process_single_datapack_metrics(dataset: str, datapack: str) -> tuple[str, dict[str, Any]]:
    loader = DatasetLoader(dataset, datapack)
    calculator = DatasetMetricsCalculator(loader)

    # Calculate metrics
    results = {}
    results["SDD@1"] = calculator.compute_sdd(k=1)
    results["SDD@3"] = calculator.compute_sdd(k=3)
    results["SDD@5"] = calculator.compute_sdd(k=5)
    results["AC"] = calculator.compute_ac()
    results["CPL"] = calculator.compute_cpl()
    results["RootServiceDegree"] = calculator.get_root_service_degree()

    return datapack, results


@app.command()
def batch_metrics(dataset: str):
    folder = Path("data/rcabench-platform-v2/data") / dataset
    if not folder.exists():
        logger.error(f"Error: Dataset {dataset} does not exist")
        return

    datapacks = [f.name for f in folder.iterdir() if f.is_dir()]

    if not datapacks:
        logger.error(f"Error: No datapacks found in dataset {dataset}")
        return

    logger.info(f"Found {len(datapacks)} datapacks: {datapacks}")

    # Create tasks for parallel processing
    tasks = [functools.partial(_process_single_datapack_metrics, dataset, datapack) for datapack in datapacks]

    cpu = os.cpu_count()
    assert cpu is not None, "CPU count is not available"

    results_list = fmap_processpool(
        tasks,
        parallel=cpu // 4,
        cpu_limit_each=4,
        ignore_exceptions=True,
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


def patch_service_name(datapack: Path, files):
    for file_group in files.values():
        for file in file_group:
            file_path = datapack / file
            assert file_path.exists(), f"File {file_path} does not exist"
            df = pl.scan_parquet(file_path)
            assert "ServiceName" in df.collect_schema().names(), f"File {file_path} does not have service_name column"
            df = df.with_columns(
                pl.when(pl.col("ServiceName") == "loadgenerator-service")
                .then(pl.lit("loadgenerator"))
                .otherwise(pl.col("ServiceName"))
                .alias("ServiceName")
            )
            df.collect().write_parquet(file_path)


@app.command()
def patch(dataset: str):
    files = get_files(dataset)
    folder = Path("data/rcabench_dataset")
    datapacks = [f for f in folder.iterdir() if f.is_dir()]

    tasks = [functools.partial(patch_service_name, datapack, files) for datapack in datapacks]

    cpu = os.cpu_count()
    assert cpu is not None, "CPU count is not available"
    fmap_processpool(
        tasks,
        parallel=cpu // 4,
        cpu_limit_each=4,
        ignore_exceptions=True,
    )


if __name__ == "__main__":
    app()
