#!/usr/bin/env -S uv run -s
# function: analyze the static distribution of datasets, including service names,
# log lines, entry traces, metric names, span names, trace length distribution, time slices, QPM, and total duration
import functools
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl
from rcabench.openapi import (
    ApiClient,
    DtoInjectionFieldMappingResp,
    HandlerNode,
    HandlerPair,
    HandlerResources,
    InjectionApi,
)

from rcabench_platform.v2.cli.main import app
from rcabench_platform.v2.datasets.spec import get_datapack_folder
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.logging import logger
from rcabench_platform.v2.metrics.dataset_loader import DatasetLoader
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
