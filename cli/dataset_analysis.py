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

    if dataset.startswith("SN") or dataset.startswith("TT"):
        return {
            "trace": ["trace.parquet"],
            "logs": ["log.parquet"],
            "metrics": ["metric.parquet"],
        }

    if dataset.startswith("rcaeval"):
        return {
            "trace": ["traces.parquet"],
            "logs": ["simple_metrics.parquet"],
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
    metadata.ServiceNames = set(df.select("service_name").unique().collect().to_series().to_list())
    metadata.SpanNames = set(df.select("span_name").unique().collect().to_series().to_list())

    metadata.EntryTrace = df.filter(pl.col("parent_span_id") == "").select(pl.len()).collect().item()

    df_collected = df.collect()
    trace_path_lengths = []

    for trace_id in df_collected.select("trace_id").unique().to_series():
        trace_spans = df_collected.filter(pl.col("trace_id") == trace_id)

        spans_dict = {}
        for row in trace_spans.iter_rows(named=True):
            spans_dict[row["span_id"]] = {"parent_span_id": row["parent_span_id"], "children": []}

        for span_id, span_info in spans_dict.items():
            parent_id = span_info["parent_span_id"]
            if parent_id is not None and parent_id in spans_dict:
                spans_dict[parent_id]["children"].append(span_id)

        root_spans = [
            span_id
            for span_id, span_info in spans_dict.items()
            if span_info["parent_span_id"] == "" or span_info["parent_span_id"] is None
        ]

        def calculate_max_depth(span_id, current_depth=1):
            children = spans_dict[span_id]["children"]
            if not children:
                return current_depth
            return max(calculate_max_depth(child, current_depth + 1) for child in children)

        max_depth = 0
        for root_span in root_spans:
            depth = calculate_max_depth(root_span)
            max_depth = max(max_depth, depth)

        trace_path_lengths.append(max_depth)

    length_dist = {}
    for length in trace_path_lengths:
        length_str = str(length)
        length_dist[length_str] = length_dist.get(length_str, 0) + 1
    metadata.TraceLengthDistribution = length_dist

    time_data = df.select("time").collect().to_series()
    if len(time_data) > 0:
        min_time = time_data.min()
        max_time = time_data.max()
        assert min_time is not None and max_time is not None, "时间数据不能为空"
        metadata.TimeSlices = [(min_time, max_time)]

    print(metadata.EntryTrace)
    return metadata


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

    return fmap_threadpool(total_tasks, parallel=3)


@app.command()
def distribution(dataset: str):
    files = get_files(dataset)
    folder = Path("data/rcabench-platform-v2/data") / dataset
    datapacks = [f for f in folder.iterdir() if f.is_dir()]

    results = fmap_processpool(
        [functools.partial(process_datapack, datapack, files) for datapack in datapacks],
        parallel=4,
    )

    results = [item for sublist in results for item in sublist]
    metadata = merge_metadata(results)

    with open(f"temp/{dataset}_metadata.json", "w") as f:
        json.dump(metadata.to_dict(), f, indent=4, default=str)


if __name__ == "__main__":
    app()
