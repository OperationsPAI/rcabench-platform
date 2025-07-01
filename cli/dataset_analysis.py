#!/usr/bin/env -S uv run -s

from typing import Any
from rcabench_platform.v2.cli.main import app, logger, timeit
from datetime import datetime
from pathlib import Path
import json
import re
import functools
import polars as pl
from rcabench_platform.v2.utils.fmap import fmap_threadpool, fmap_processpool
from dataclasses import dataclass, field
import os


def extract_path(uri: str):
    from rcabench_platform.v2.datasets.train_ticket import PATTERN_REPLACEMENTS

    for pattern, replacement in PATTERN_REPLACEMENTS:
        res = re.sub(pattern, replacement, uri)
        if res != uri:
            return res
    return uri


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
            "logs": ["simple_metrics.parquet"],
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
    ServiceNames: set = field(default_factory=set)
    LogLines: int = 0
    EntryTrace: int = 0
    MetricNames: set = field(default_factory=set)
    SpanNames: set = field(default_factory=set)
    TraceLengthDistribution: dict[str, int] = field(default_factory=dict)
    TimeSlices: list[tuple[Any, Any]] = field(default_factory=list)
    QPM: float = 0.0

    def to_dict(self):
        return {
            "ServiceNames": list(self.ServiceNames),
            "LogLines": self.LogLines,
            "EntryTrace": self.EntryTrace,
            "MetricNames": list(self.MetricNames),
            "SpanNames": list(self.SpanNames),
            "TraceLengthDistribution": self.TraceLengthDistribution,
            "TimeSlices": self.TimeSlices,
            "QPM": self.QPM,
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
        assert min_time is not None and max_time is not None, "时间数据不能为空"
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
        assert min_time is not None and max_time is not None, "时间数据不能为空"
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
            pl.when(pl.col("parent_span_id") == "").then(1).otherwise(0).sum().alias("entry_trace_count"),
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

    print(metadata.EntryTrace)
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
            if start <= current_end:  # 重叠或相邻
                current_end = max(current_end, end)
            else:  # 不重叠，保存当前区间并开始新区间
                merged_slices.append((current_start, current_end))
                current_start, current_end = start, end

        merged_slices.append((current_start, current_end))
        merged.TimeSlices = merged_slices

        # 计算 QPM (Queries Per Minute)
        total_time_minutes = 0
        for start, end in merged.TimeSlices:
            # 假设时间是以秒为单位的时间戳或datetime对象
            if hasattr(start, "timestamp"):
                duration_seconds = end.timestamp() - start.timestamp()
            else:
                duration_seconds = end - start
            total_time_minutes += duration_seconds / 60.0

        if total_time_minutes > 0:
            merged.QPM = merged.EntryTrace / total_time_minutes

    return merged


def process_datapack(datapack: Path, files):
    trace_tasks = [functools.partial(_scan_trace, datapack / f) for f in files["trace"]]
    log_tasks = [functools.partial(_scan_log, datapack / f) for f in files["logs"]]
    metric_tasks = [functools.partial(_scan_metric, datapack / f) for f in files["metrics"]]
    total_tasks = trace_tasks + log_tasks + metric_tasks
    return total_tasks


def patch_service_name(datapack: Path, files):
    for file_group in files.values():
        for file in file_group:
            file_path = datapack / file
            assert file_path.exists()
            df = pl.scan_parquet(file_path)
            assert "service_name" in df.collect_schema().names(), f"File {file_path} does not have service_name column"
            df = df.with_columns(
                pl.col("service_name").str.replace(r"^ts[0-5]-", "", literal=False).alias("service_name")
            )
            df.collect().write_parquet(file_path)


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
def patch(dataset: str):
    files = get_files(dataset)
    folder = Path("data/rcabench-platform-v2/data") / dataset
    datapacks = [f for f in folder.iterdir() if f.is_dir()]

    tasks = [functools.partial(patch_service_name, datapack, files) for datapack in datapacks]

    fmap_processpool(
        tasks,
        parallel=4,
    )


if __name__ == "__main__":
    app()
