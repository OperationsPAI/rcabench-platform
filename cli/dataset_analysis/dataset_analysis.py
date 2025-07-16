#!/usr/bin/env -S uv run -s
# function: analyze the static distribution of datasets, including service names,
# log lines, entry traces, metric names, span names, trace length distribution, time slices, QPM, and total duration
from typing import Any
from rcabench_platform.v2.cli.main import app
from pathlib import Path
import json
import re
import functools
import polars as pl
from rcabench_platform.v2.utils.fmap import fmap_processpool
from dataclasses import dataclass, field
import os
import networkx as nx
from collections import defaultdict
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import graphviz


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
    ServiceNames: set = field(default_factory=set)
    LogLines: int = 0
    EntryTrace: int = 0
    MetricNames: set = field(default_factory=set)
    SpanNames: set = field(default_factory=set)
    TraceLengthDistribution: dict[str, int] = field(default_factory=dict)
    TimeSlices: list[tuple[Any, Any]] = field(default_factory=list)
    QPM: float = 0.0
    TotalDurationSeconds: float = 0.0  # 新增字段

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
            "TotalDurationSeconds": self.TotalDurationSeconds,  # 新增输出
        }


@dataclass
class FaultMetrics:
    causal_path_length: int = 0  # CPL: 从根因服务到症状服务的最短路径长度
    blast_radius: float = 0.0  # BR: 受影响服务的比例
    centrality_stress: float = 0.0  # CS: 根因服务的介数中心性

    def to_dict(self):
        return {
            "causal_path_length": self.causal_path_length,
            "blast_radius": self.blast_radius,
            "centrality_stress": self.centrality_stress,
        }


@dataclass
class DatasetMetrics:
    fault_metrics: list[FaultMetrics] = field(default_factory=list)
    avg_cpl: float = 0.0
    avg_br: float = 0.0
    avg_cs: float = 0.0

    def calculate_averages(self):
        if not self.fault_metrics:
            return

        self.avg_cpl = sum(m.causal_path_length for m in self.fault_metrics) / len(self.fault_metrics)
        self.avg_br = sum(m.blast_radius for m in self.fault_metrics) / len(self.fault_metrics)
        self.avg_cs = sum(m.centrality_stress for m in self.fault_metrics) / len(self.fault_metrics)

    def to_dict(self):
        return {
            "fault_metrics": [m.to_dict() for m in self.fault_metrics],
            "avg_cpl": self.avg_cpl,
            "avg_br": self.avg_br,
            "avg_cs": self.avg_cs,
            "total_faults": len(self.fault_metrics),
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

        merged.TotalDurationSeconds = total_time_seconds  # 赋值

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

    with open(f"intermediate_results/{dataset}_metadata.json", "w") as f:
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


def build_service_dependency_graph(traces_df: pl.DataFrame) -> nx.DiGraph:
    graph = nx.DiGraph()

    for trace_group in traces_df.group_by("trace_id", maintain_order=False):
        trace_data = trace_group[1]
        spans = {}
        for row in trace_data.iter_rows(named=True):
            span_id = row["span_id"]
            parent_span_id = row["parent_span_id"]
            service_name = row["service_name"]

            spans[span_id] = {"service": service_name, "parent": parent_span_id if parent_span_id else None}

        for span_id, span_info in spans.items():
            if span_info["parent"]:
                parent_span = spans.get(span_info["parent"])
                if parent_span and parent_span["service"] != span_info["service"]:
                    graph.add_edge(parent_span["service"], span_info["service"])

    return graph


def find_entry_service(traces_df: pl.DataFrame) -> str:
    """找到入口服务（通常是用户直接访问的服务）"""
    # 找到所有根span（没有parent的span）的服务
    root_spans = traces_df.filter((pl.col("parent_span_id") == "") | pl.col("parent_span_id").is_null())

    # 统计根span最多的服务作为入口服务
    entry_service_counts = (
        root_spans.group_by("service_name").agg(pl.len().alias("count")).sort("count", descending=True)
    )

    if entry_service_counts.height > 0:
        return entry_service_counts.row(0)[0]  # 返回count最高的服务名

    # 如果没有找到，返回第一个服务
    first_service = traces_df.select("service_name").limit(1).item()
    return first_service


def calculate_blast_radius(traces_df: pl.DataFrame, baseline_traces_df: pl.DataFrame) -> float:
    abnormal_df = traces_df
    normal_df = baseline_traces_df

    def calculate_service_metrics(df: pl.DataFrame) -> pl.DataFrame:
        return df.group_by("service_name").agg(
            [
                pl.col("duration").mean().alias("avg_duration"),
                pl.col("duration").median().alias("median_duration"),
                pl.col("duration").quantile(0.95).alias("p95_duration"),
                pl.when(pl.col("attr.status_code").is_in(["OK", "Unset", ""]))
                .then(1)
                .otherwise(0)
                .mean()
                .alias("success_rate"),
                pl.len().alias("request_count"),
            ]
        )

    normal_metrics = calculate_service_metrics(normal_df)
    abnormal_metrics = calculate_service_metrics(abnormal_df)

    comparison = normal_metrics.join(abnormal_metrics, on="service_name", how="inner", suffix="_abnormal").with_columns(
        [
            (pl.col("avg_duration_abnormal") / pl.col("avg_duration")).alias("avg_duration_ratio"),
            (pl.col("median_duration_abnormal") / pl.col("median_duration")).alias("median_duration_ratio"),
            (pl.col("p95_duration_abnormal") / pl.col("p95_duration")).alias("p95_duration_ratio"),
            (pl.col("success_rate") - pl.col("success_rate_abnormal")).alias("success_rate_drop"),
        ]
    )

    LATENCY_THRESHOLD = 1.5
    SUCCESS_RATE_THRESHOLD = 0.05

    affected_services = comparison.filter(
        (pl.col("avg_duration_ratio") > LATENCY_THRESHOLD)  # 平均延迟显著增加
        | (pl.col("p95_duration_ratio") > LATENCY_THRESHOLD)  # P95延迟显著增加
        | (pl.col("success_rate_drop") > SUCCESS_RATE_THRESHOLD)  # 成功率显著下降
    )

    all_comparable_services = comparison.height

    if all_comparable_services == 0:
        return 0.0

    affected_count = affected_services.height
    return affected_count / all_comparable_services


def calculate_fault_metrics(datapack_path: Path) -> FaultMetrics | None:
    metrics = FaultMetrics()

    injection_file = datapack_path / "injection.json"
    assert injection_file.exists(), f"Injection file not found in {datapack_path}"

    with open(injection_file) as f:
        injection = json.load(f)
        root_cause_service = injection["ground_truth"]["service"]

    assert root_cause_service, f"{injection}"

    abnormal_traces_file = datapack_path / "abnormal_traces.parquet"
    assert abnormal_traces_file.exists(), f"Abnormal traces file not found in {datapack_path}"

    abnormal_traces = pl.scan_parquet(abnormal_traces_file).collect()

    entry_service = find_entry_service(abnormal_traces)
    normal_traces_file = datapack_path / "normal_traces.parquet"
    baseline_traces = None
    assert normal_traces_file.exists(), f"Normal traces file not found in {datapack_path}"
    baseline_traces = pl.scan_parquet(normal_traces_file).collect()
    assert baseline_traces is not None, "Baseline traces must be provided"

    combined_traces = pl.concat([baseline_traces, abnormal_traces])
    dependency_graph = build_service_dependency_graph(combined_traces)

    try:
        assert entry_service in dependency_graph

        for rc_service in root_cause_service:
            if rc_service == "mysql":
                continue
            assert rc_service in dependency_graph

        undirected_graph = dependency_graph.to_undirected()

        path_lengths = []
        for rc_service in root_cause_service:
            try:
                if rc_service == "mysql":
                    continue
                path_length = nx.shortest_path_length(undirected_graph, source=entry_service, target=rc_service)
                path_lengths.append(path_length)
                print(f"Found undirected path from {entry_service} to {rc_service}: length {path_length}")
            except nx.NetworkXNoPath:
                print(f"No path found between {entry_service} and {rc_service}")
                continue

        if path_lengths:
            metrics.causal_path_length = min(path_lengths)
            print(f"Minimum causal path length: {metrics.causal_path_length}")
        else:
            raise nx.NetworkXNoPath("No paths found to any root cause service")
    except (nx.NetworkXNoPath, AssertionError) as e:
        if isinstance(e, AssertionError):
            print(f"Assertion failed: entry_service={entry_service}, root_cause_service={root_cause_service}")
        else:
            print(f"No path found between {entry_service} and {root_cause_service} in undirected graph.")

        dot = graphviz.Digraph(comment="Service Dependency Graph")
        dot.attr(rankdir="TB")  # Top to Bottom layout
        dot.attr("node", shape="box", style="rounded,filled", fontname="Arial")
        dot.attr("edge", fontname="Arial")

        dot.attr(size="12,8")
        dot.attr(dpi="300")
        dot.attr(bgcolor="white")

        for node in dependency_graph.nodes():
            if node in root_cause_service:
                dot.node(node, node, fillcolor="lightcoral", color="red", penwidth="3")
            elif node == entry_service:
                dot.node(node, node, fillcolor="lightgreen", color="green", penwidth="3")
            else:
                dot.node(node, node, fillcolor="lightblue", color="black")

        for edge in dependency_graph.edges():
            dot.edge(edge[0], edge[1])

        # 添加图例节点
        with dot.subgraph(name="cluster_legend") as legend:  # type: ignore
            legend.attr(label="Legend", style="rounded", color="gray")
            legend.node(
                "legend_root",
                f"Root Cause\n{', '.join(root_cause_service)}",
                fillcolor="lightcoral",
                color="red",
                penwidth="3",
            )
            legend.node(
                "legend_entry", f"Entry Service\n{entry_service}", fillcolor="lightgreen", color="green", penwidth="3"
            )
            legend.node("legend_other", "Other Services", fillcolor="lightblue", color="black")

        debug_dir = Path("temp/debug_graphs")
        debug_dir.mkdir(exist_ok=True)
        output_file = debug_dir / f"{datapack_path.name}_no_path"

        dot.render(str(output_file), format="png", cleanup=True)
        print(f"Debug graph saved to: {output_file}.png")

        metrics.causal_path_length = -1
        return None

    metrics.blast_radius = calculate_blast_radius(abnormal_traces, baseline_traces)

    betweenness = nx.betweenness_centrality(dependency_graph, normalized=True)
    centrality_scores = [betweenness.get(rc_service, 0.0) for rc_service in root_cause_service]
    metrics.centrality_stress = sum(centrality_scores) / len(centrality_scores) if centrality_scores else 0.0

    return metrics


@app.command()
def dataset_metric(dataset: str):
    folder = Path("data/rcabench-platform-v2/data") / dataset
    datapacks = [f for f in folder.iterdir() if f.is_dir()]

    dataset_metrics = DatasetMetrics()

    tasks = [functools.partial(calculate_fault_metrics, datapack) for datapack in datapacks]

    print(f"Processing {len(tasks)} datapacks in parallel...")

    fault_metrics_results = fmap_processpool(
        tasks,
        parallel=16,
    )

    for fault_metrics_results in fault_metrics_results:
        if fault_metrics_results is not None:
            dataset_metrics.fault_metrics.append(fault_metrics_results)

    dataset_metrics.calculate_averages()

    output_file = f"temp/{dataset}_fault_metrics.json"
    with open(output_file, "w") as f:
        json.dump(dataset_metrics.to_dict(), f, indent=4, default=str)

    print(f"Fault metrics saved to {output_file}")
    print(f"Dataset: {dataset}")
    print(f"Total faults: {len(dataset_metrics.fault_metrics)}")
    print(f"Average CPL: {dataset_metrics.avg_cpl:.2f}")
    print(f"Average BR: {dataset_metrics.avg_br:.2f}")
    print(f"Average CS: {dataset_metrics.avg_cs:.2f}")

    return dataset_metrics


@app.command()
def local_test():
    dataset_metrics = DatasetMetrics()
    path = Path("/mnt/jfs/rcabench_dataset/ts5-ts-verification-code-service-return-f62ks9/converted")
    fault_metrics_results = calculate_fault_metrics(path)

    if fault_metrics_results is not None:
        dataset_metrics.fault_metrics.append(fault_metrics_results)

    dataset_metrics.calculate_averages()

    print(f"Total faults: {len(dataset_metrics.fault_metrics)}")
    print(f"Average CPL: {dataset_metrics.avg_cpl:.2f}")
    print(f"Average BR: {dataset_metrics.avg_br:.2f}")
    print(f"Average CS: {dataset_metrics.avg_cs:.2f}")

    return dataset_metrics


if __name__ == "__main__":
    app()
