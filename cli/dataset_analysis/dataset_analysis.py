#!/usr/bin/env -S uv run -s
# function: analyze the static distribution of datasets, including service names,
# log lines, entry traces, metric names, span names, trace length distribution, time slices, QPM, and total duration
import functools
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from rcabench.openapi import (
    ApiClient,
    DtoInjectionFieldMappingResp,
    DtoInjectionV2CustomLabelManageReq,
    DtoLabelItem,
    HandlerNode,
    HandlerResources,
    InjectionApi,
    InjectionsApi,
)

from rcabench_platform.v2.cli.main import app
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.datasets.rcabench import RCABenchAnalyzerLoader, valid
from rcabench_platform.v2.datasets.rcaeval import RCAEvalAnalyzerLoader
from rcabench_platform.v2.datasets.spec import get_datapack_folder
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.logging import logger
from rcabench_platform.v2.metrics.metrics_calculator import DatasetMetricsCalculator
from rcabench_platform.v2.utils.fmap import fmap_processpool


@dataclass
class CountItem:
    fault_type: str = ""
    service: str = ""
    node: HandlerNode | None = None
    is_pair: bool = False


@dataclass
class CoverageItem:
    num: int = 0
    range_num: int = 0
    covered_mapping: dict[str, bool] = field(default_factory=dict)


@dataclass
class PairStats:
    in_degree: int = 0
    out_degree: int = 0


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


@dataclass
class Distribution:
    faults: dict[str, int] = field(default_factory=dict)
    services: dict[str, int] = field(default_factory=dict)
    fault_services: dict[str, dict[str, int]] = field(default_factory=dict)
    pairs: dict[str, PairStats] = field(default_factory=dict)
    fault_service_attribute_coverages: dict[str, dict[str, float]] = field(default_factory=dict)
    fault_pair_attribute_coverages: dict[str, dict[str, float]] = field(default_factory=dict)


class Analyzer:
    """
    Analyzes the static distribution of datasets, including service names,
    log lines, entry traces, metric names, span names, trace length distribution,
    time slices, QPM, and total duration.
    """

    def __init__(self, client: ApiClient, namespace: str = "default", nodes: list[HandlerNode] = []):
        """
        Initialize the analyzer with a list of nodes.
        """
        self.namespace = namespace
        self.nodes = nodes

        self.injector = InjectionApi(client)

        self._get_conf()
        self._get_resources()

    def _get_conf(self) -> None:
        resp = self.injector.api_v1_injections_conf_get(namespace=self.namespace)
        assert resp.data is not None
        self.conf = resp.data

    def _get_resources(self) -> None:
        resp = self.injector.api_v1_injections_mapping_get()
        assert resp.data is not None
        self.injection_mapping: DtoInjectionFieldMappingResp = resp.data

        resp = self.injector.api_v1_injections_ns_resources_get(namespace=self.namespace)
        assert resp.data is not None
        self.injection_resources: HandlerResources = resp.data

    def _get_individual_service(self, individual: HandlerNode) -> tuple[str, bool]:
        fault_type_index = str(individual.value)

        assert self.injection_mapping.fault_type is not None
        assert self.injection_mapping.fault_resource is not None
        assert individual.children is not None

        fault_type: str = self.injection_mapping.fault_type[fault_type_index]
        fault_resource_meta: dict[str, Any] = self.injection_mapping.fault_resource[fault_type]
        fault_resource_name: str = fault_resource_meta["name"]
        fault_resource = self.injection_resources.to_dict().get(fault_resource_name)

        child_node = individual.children[fault_type_index]
        assert child_node.children is not None
        service_index = child_node.children["2"].value

        assert fault_resource is not None
        assert service_index is not None
        assert service_index < len(fault_resource), (
            f"Service index {service_index} out of bounds for fault resource {len(fault_resource)}"
        )

        service: str | dict[str, Any] = fault_resource[service_index]
        if isinstance(service, str):
            return service, False

        assert "source" in service and "target" in service, (
            f"Service source or target is None for fault {fault_type} with index {service_index}"
        )
        return f"{service['source']}->{service['target']}", True

    @staticmethod
    def _recursive_to_get_range_num(node: HandlerNode, key: str) -> int:
        int_key = int(key)
        total = 0
        if node.children is None and int_key > 2:
            if node.range is not None:
                total = node.range[1] - node.range[0] + 1

        if node.children is not None:
            for childKey, childNode in node.children.items():
                total += Analyzer._recursive_to_get_range_num(childNode, childKey)

        return total

    @staticmethod
    def _recursive_to_get_covered_mapping(node: HandlerNode, key: str) -> dict[str, bool]:
        int_key = int(key)
        covered_mapping: dict[str, bool] = {}
        if node.children is None and int_key > 2:
            covered_mapping[f"{key}-{node.value}"] = True

        if node.children is not None:
            for childKey, childNode in node.children.items():
                assert childNode is not None, f"Child node for key {childKey} is None"
                covered_mapping.update(Analyzer._recursive_to_get_covered_mapping(childNode, childKey))

        return covered_mapping

    def get_distribution(self) -> Distribution:
        """
        Get the distribution of faults and services from the nodes.
        """
        distribution = Distribution()

        count_items: list[CountItem] = []
        for node in self.nodes:
            fault = node.value
            assert fault is not None, "Node value must not be None"
            assert self.injection_mapping.fault_type is not None, "Fault type mapping must not be None"
            fault_type = self.injection_mapping.fault_type.get(str(fault), "unknown")
            service, is_pair = self._get_individual_service(node)
            count_items.append(CountItem(fault_type=fault_type, service=service, node=node, is_pair=is_pair))

        for item in count_items:
            # Count faults
            distribution.faults[item.fault_type] = distribution.faults.get(item.fault_type, 0) + 1

            # Count services
            if not item.is_pair:
                distribution.services[item.service] = distribution.services.get(item.service, 0) + 1
            else:
                source, target = item.service.split("->")
                if source not in distribution.pairs:
                    distribution.pairs[source] = PairStats(out_degree=1)
                else:
                    distribution.pairs[source].out_degree += 1

                if target not in distribution.pairs:
                    distribution.pairs[target] = PairStats(in_degree=1)
                else:
                    distribution.pairs[target].in_degree += 1

            # Count fault-service pairs
            if not item.is_pair:
                if item.fault_type not in distribution.fault_services:
                    distribution.fault_services[item.fault_type] = {}

                distribution.fault_services[item.fault_type][item.service] = (
                    distribution.fault_services[item.fault_type].get(item.service, 0) + 1
                )

        fault_range_num_mapping: dict[str, int] = {}
        assert self.conf.children is not None, "Injection configuration children must not be None"
        for key, node in self.conf.children.items():
            range_num = self._recursive_to_get_range_num(node, key)
            fault_range_num_mapping[key] = range_num

        coverage_item_mapping: dict[str, dict[str, CoverageItem]] = {}
        for item in count_items:
            assert item.node is not None, "Node must not be None"
            covered_mapping = self._recursive_to_get_covered_mapping(item.node, str(item.node.value))

            if item.fault_type not in coverage_item_mapping:
                coverage_item_mapping[item.fault_type] = {}
                coverage_item_mapping[item.fault_type][item.service] = CoverageItem(
                    range_num=fault_range_num_mapping.get(str(item.node.value), 0),
                    covered_mapping=covered_mapping,
                )

            if item.service not in coverage_item_mapping[item.fault_type]:
                coverage_item_mapping[item.fault_type][item.service] = CoverageItem(
                    range_num=fault_range_num_mapping.get(str(item.node.value), 0),
                    covered_mapping=covered_mapping,
                )

            coverage_item = coverage_item_mapping[item.fault_type][item.service]
            coverage_item.num += 1

        for fault_type, service_coverages in coverage_item_mapping.items():
            for service, coverage_item in service_coverages.items():
                ratio = 0.0
                if coverage_item.range_num != 0:
                    ratio = len(coverage_item.covered_mapping) / coverage_item.range_num

                is_pair = "->" in service
                if is_pair:
                    if fault_type not in distribution.fault_pair_attribute_coverages:
                        distribution.fault_pair_attribute_coverages[fault_type] = {}
                    distribution.fault_pair_attribute_coverages[fault_type][service] = ratio
                else:
                    if fault_type not in distribution.fault_service_attribute_coverages:
                        distribution.fault_service_attribute_coverages[fault_type] = {}
                    distribution.fault_service_attribute_coverages[fault_type][service] = ratio

        return distribution


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


def _process_single_datapack_metrics(dataset: str, datapack: str) -> tuple[str, dict[str, Any]]:
    if dataset == "rcabench":
        loader = RCABenchAnalyzerLoader(dataset, datapack)
    elif dataset.startswith("rcaeval"):
        loader = RCAEvalAnalyzerLoader(dataset, datapack)
    else:
        assert False, f"Unknown dataset: {dataset}"

    try:
        calculator = DatasetMetricsCalculator(loader)
    except Exception as e:
        logger.error(f"Error processing datapack {datapack} in dataset {dataset}: {e}")
        return datapack, {}

    # Calculate metrics
    results = {}
    results["SDD@1"] = calculator.compute_sdd(k=1)
    results["SDD@3"] = calculator.compute_sdd(k=3)
    results["SDD@5"] = calculator.compute_sdd(k=5)
    results["AC"] = calculator.compute_ac()
    results["CPL"] = calculator.compute_cpl()
    results["RootServiceDegree"] = calculator.get_root_cause_degree()

    with RCABenchClient() as client:
        api = InjectionsApi(client)
        api.api_v2_injections_name_labels_patch(
            name=datapack,
            manage=DtoInjectionV2CustomLabelManageReq(
                add_labels=[
                    DtoLabelItem(key="SDD@1", value=str(results["SDD@1"])),
                    DtoLabelItem(key="SDD@3", value=str(results["SDD@3"])),
                    DtoLabelItem(key="SDD@5", value=str(results["SDD@5"])),
                    DtoLabelItem(key="CPL", value=str(results["CPL"])),
                    DtoLabelItem(key="RootServiceDegree", value=str(results["RootServiceDegree"])),
                ]
            ),
        )

    return datapack, results


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

            page = 1
            page_size = 100

            while True:
                logger.info(f"Fetching page {page} with {page_size} items per page...")
                res = injection_api.api_v2_injections_get(tags=["absolute_anomaly"], size=page_size, page=page)
                assert res.data is not None, "API returned empty data"

                if res.data.items is None or len(res.data.items) == 0:
                    logger.info(f"Page {page} has no data, stopping...")
                    break

                current_datapacks = [i.injection_name for i in res.data.items if i.injection_name is not None]
                datapacks.extend(current_datapacks)
                logger.info(f"Page {page} returned {len(current_datapacks)} valid items")

                if res.data.pagination is not None and res.data.pagination.total_pages is not None:
                    if page >= res.data.pagination.total_pages:
                        logger.info(f"Retrieved all {res.data.pagination.total_pages} pages")
                        break

                if len(res.data.items) < page_size:
                    logger.info("Last page reached")
                    break

                page += 1

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
