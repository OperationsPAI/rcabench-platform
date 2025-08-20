#!/usr/bin/env -S uv run -s
# function: analyze the static distribution of datasets, including service names,
# log lines, entry traces, metric names, span names, trace length distribution, time slices, QPM, and total duration
import functools
import json
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import polars as pl
from rcabench.openapi import (
    ApiClient,
    DtoGranularityRecord,
    DtoInjectionFieldMappingResp,
    DtoInjectionV2CustomLabelManageReq,
    DtoInjectionV2Response,
    DtoLabelItem,
    EvaluationApi,
    HandlerNode,
    HandlerResources,
    InjectionApi,
    InjectionsApi,
)

from rcabench_platform.v2.cli.main import app
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.datasets.rcabench import RCABenchAnalyzerLoader
from rcabench_platform.v2.datasets.rcaeval import RCAEvalAnalyzerLoader
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.logging import logger
from rcabench_platform.v2.metrics.algo_metrics import calculate_metrics_for_level
from rcabench_platform.v2.metrics.metrics_calculator import DatasetMetricsCalculator
from rcabench_platform.v2.utils.fmap import fmap_processpool


@dataclass
class CountItem:
    node: HandlerNode
    injection: DtoInjectionV2Response
    fault_type: str = ""
    service: str = ""
    is_pair: bool = False
    metrics: dict[str, int] = field(default_factory=dict)
    algo_evals: dict[str, list[DtoGranularityRecord]] = field(default_factory=dict)

    service_names: set[str] = field(default_factory=set)
    service_names_by_trace: set[str] = field(default_factory=set)  # trace
    trace_length: Counter[int] = field(default_factory=Counter)
    log_lines: dict[str, int] = field(default_factory=dict)  # service_name -> log_lines
    metric_count: dict[str, int] = field(default_factory=dict)  # metric_name -> count
    duration: timedelta = timedelta(seconds=0)  # duration in seconds
    trace_count: int = 0  # number of traces
    anomaly_degree: Literal["absolute", "may", "no"] = "no"
    workload: Literal["trainticket"] = "trainticket"

    @property
    def qps(self) -> float:
        if self.duration > timedelta(seconds=0):
            return self.trace_count / self.duration.total_seconds()
        return 0.0

    @property
    def qpm(self) -> float:
        if self.duration > timedelta(seconds=0):
            return self.trace_count / self.duration.total_seconds() * 60
        return 0.0

    @property
    def service_coverage(self) -> float:
        return len(self.service_names_by_trace) / len(self.service_names)


@dataclass
class CoverageItem:
    is_pair: bool = False
    num: int = 0
    range_num: int = 0
    covered_mapping: dict[str, bool] = field(default_factory=dict)


@dataclass
class PairStats:
    in_degree: int = 0
    out_degree: int = 0


@dataclass
class Distribution:
    faults: dict[str, int] = field(default_factory=dict)
    services: dict[str, int] = field(default_factory=dict)
    pairs: dict[str, PairStats] = field(default_factory=dict)
    metrics: dict[str, dict[str, int]] = field(default_factory=dict)

    fault_services: dict[str, dict[str, int]] = field(default_factory=dict)
    fault_service_attribute_coverages: dict[str, dict[str, float]] = field(default_factory=dict)
    fault_service_metrics: dict[str, dict[str, dict[str, dict[str, int]]]] = field(default_factory=dict)

    fault_pair_attribute_coverages: dict[str, dict[str, float]] = field(default_factory=dict)
    fault_pair_metrics: dict[str, dict[str, dict[str, dict[str, int]]]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "faults": self.faults,
            "services": self.services,
            "pairs": {k: v.__dict__ for k, v in self.pairs.items()},
            "metrics": self.metrics,
            "fault_pair_attribute_coverages": self.fault_pair_attribute_coverages,
            "fault_pair_metrics": {k: {sk: sv for sk, sv in v.items()} for k, v in self.fault_pair_metrics.items()},
            "fault_services": self.fault_services,
            "fault_service_attribute_coverages": self.fault_service_attribute_coverages,
            "fault_service_metrics": {
                k: {sk: sv for sk, sv in v.items()} for k, v in self.fault_service_metrics.items()
            },
        }


class Analyzer:
    """
    Analyzes the static distribution of datasets, including service names,
    log lines, entry traces, metric names, span names, trace length distribution,
    time slices, QPM, and total duration.
    """

    def __init__(
        self,
        client: ApiClient,
        namespace: str,
        metrics: list[str],
        algorithms: list[str],
        injections: list[DtoInjectionV2Response],
    ):
        self.evaluator = EvaluationApi(client)
        self.injector = InjectionApi(client)
        self.namespace = namespace
        self.algorithms = algorithms
        self.metrics = metrics

        self.conf = self._get_conf()
        self.injection_mapping, self.injection_resources = self._get_resources()

        self.count_items = self._get_count_items(injections)
        self.coverage_items = self._get_coverage_items()

    def _get_conf(self) -> HandlerNode:
        resp = self.injector.api_v1_injections_conf_get(namespace=self.namespace)
        assert resp.data is not None
        return resp.data

    def _get_resources(self) -> tuple[DtoInjectionFieldMappingResp, HandlerResources]:
        resp = self.injector.api_v1_injections_mapping_get()
        assert resp.data is not None
        mapping_data = resp.data

        resp = self.injector.api_v1_injections_ns_resources_get(namespace=self.namespace)
        assert resp.data is not None
        resources_data = resp.data

        return mapping_data, resources_data

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

    def _process_item(self, injection: DtoInjectionV2Response) -> CountItem | None:
        if not injection.engine_config or not injection.injection_name:
            return None

        datapack_path = Path("data/rcabench_dataset") / injection.injection_name / "converted"

        node: HandlerNode
        fault_type: str = ""
        service: str = ""
        is_pair: bool = False

        node = HandlerNode.from_json(str(injection.engine_config))
        fault = node.value
        assert fault is not None, "Node value must not be None"
        assert self.injection_mapping.fault_type is not None, "Fault type mapping must not be None"
        fault_type = self.injection_mapping.fault_type[str(fault)]
        service, is_pair = self._get_individual_service(node)

        assert injection.labels is not None
        tags = [label.value for label in injection.labels if label.key == "tag"]
        label_mapping = {label.key: label.value for label in injection.labels if label.key}

        metric_values: dict[str, int] = {}

        for metric in self.metrics:
            value = 0
            value_str = label_mapping.get(metric)
            if value_str is not None:
                try:
                    value = int(value_str)
                except ValueError:
                    logger.warning(f"Invalid {metric} value: {label_mapping[metric]} for injection {injection.id}")

            metric_values[metric] = value

        # algo_evals: dict[str, list[DtoGranularityRecord]] = {}
        # if injection.injection_name is not None:
        #     for algorithm in self.algorithms:
        #         resp = self.evaluator.api_v2_evaluations_algorithms_algorithm_datapacks_datapack_get(
        #             algorithm=algorithm,
        #             datapack=injection.injection_name,
        #         )
        #         assert resp.data is not None and resp.data is not None, "Failed to get evaluation data"
        #         assert resp.data.predictions is not None, "Predictions must not be None"
        #         algo_evals[algorithm] = resp.data.predictions
        service_names: set[str] = set()
        service_names_by_trace: set[str] = set()
        trace_length: Counter[int] = Counter()
        log_lines: dict[str, int] = {}
        duration: timedelta = timedelta(seconds=0)
        trace_count: int = 0

        metric_df = pl.concat(
            [
                pl.scan_parquet(datapack_path / "normal_metrics.parquet"),
                pl.scan_parquet(datapack_path / "abnormal_metrics.parquet"),
                pl.scan_parquet(datapack_path / "normal_metrics_sum.parquet"),
                pl.scan_parquet(datapack_path / "abnormal_metrics_sum.parquet"),
            ]
        )

        service_names.update(set(metric_df.select("service_name").unique().collect().to_series().to_list()))

        metric_count_df = metric_df.select("metric").collect()
        metric_count = dict(metric_count_df.group_by("metric").agg(pl.len().alias("count")).iter_rows())

        trace_df = pl.concat(
            [
                pl.scan_parquet(datapack_path / "normal_traces.parquet"),
                pl.scan_parquet(datapack_path / "abnormal_traces.parquet"),
            ]
        )

        trace_service_names = set(trace_df.select("service_name").unique().collect().to_series().to_list())
        service_names_by_trace.update(trace_service_names)
        service_names.update(trace_service_names)

        trace_count = (
            trace_df.filter((pl.col("parent_span_id") == "").or_(pl.col("parent_span_id").is_null()))
            .select(pl.len())
            .collect()
            .item()
        )

        trace_spans = trace_df.select(["trace_id", "span_id", "parent_span_id"]).collect()
        depth_results = _calculate_trace_depths_vectorized(trace_spans)
        trace_length = Counter(depth_results)

        min_time = trace_df.select(pl.col("time").min().alias("min_time")).collect().item()
        max_time = trace_df.select(pl.col("time").max().alias("max_time")).collect().item()
        duration = max_time - min_time

        log_df = pl.concat(
            [
                pl.scan_parquet(datapack_path / "normal_logs.parquet"),
                pl.scan_parquet(datapack_path / "abnormal_logs.parquet"),
            ]
        )

        log_service_counts = log_df.group_by("service_name").agg(pl.len().alias("count")).collect()
        log_lines = {row["service_name"]: row["count"] for row in log_service_counts.iter_rows(named=True)}
        log_service_names = set(log_df.select("service_name").unique().collect().to_series().to_list())
        service_names.update(log_service_names)
        service_names.remove("")

        anomaly_degree = "no"
        if "absolute_anomaly" in tags:
            anomaly_degree = "absolute"
        elif "may_anomaly" in tags:
            anomaly_degree = "may"

        return CountItem(
            node=node,
            injection=injection,
            fault_type=fault_type,
            service=service,
            is_pair=is_pair,
            metrics=metric_values,
            service_names=service_names,
            service_names_by_trace=service_names_by_trace,
            trace_length=trace_length,
            log_lines=log_lines,
            metric_count=metric_count,
            duration=duration,
            trace_count=trace_count,
            anomaly_degree=anomaly_degree,
            # algo_evals=algo_evals,
        )

    def _get_count_items(self, injections: list[DtoInjectionV2Response]) -> list[CountItem]:
        count_items: list[CountItem] = []

        for injection in injections:
            item = self._process_item(injection)
            if item is not None:
                count_items.append(item)

        return count_items

    def _get_coverage_items(self) -> dict[str, dict[str, CoverageItem]]:
        def get_range_num(node: HandlerNode, key: str) -> int:
            int_key = int(key)
            total = 0
            if node.children is None and int_key > 2:
                if node.range is not None:
                    total = node.range[1] - node.range[0] + 1

            if node.children is not None:
                for childKey, childNode in node.children.items():
                    total += get_range_num(childNode, childKey)

            return total

        def get_covered_mapping(node: HandlerNode, key: str) -> dict[str, bool]:
            int_key = int(key)
            covered_mapping: dict[str, bool] = {}
            if node.children is None and int_key > 2:
                covered_mapping[f"{key}-{node.value}"] = True

            if node.children is not None:
                for childKey, childNode in node.children.items():
                    assert childNode is not None, f"Child node for key {childKey} is None"
                    covered_mapping.update(get_covered_mapping(childNode, childKey))

            return covered_mapping

        fault_range_mapping: dict[str, int] = {}
        assert self.conf.children is not None, "Injection configuration children must not be None"

        for key, node in self.conf.children.items():
            range_num = get_range_num(node, key)
            fault_range_mapping[key] = range_num

        coverage_items: dict[str, dict[str, CoverageItem]] = {}
        for item in self.count_items:
            covered_mapping = get_covered_mapping(item.node, str(item.node.value))

            if item.fault_type not in coverage_items:
                coverage_items[item.fault_type] = {}

            if item.service not in coverage_items[item.fault_type]:
                coverage_items[item.fault_type][item.service] = CoverageItem(
                    is_pair="->" in item.service,
                    range_num=fault_range_mapping.get(str(item.node.value), 0),
                    covered_mapping=covered_mapping,
                )

            coverage_item = coverage_items[item.fault_type][item.service]
            coverage_item.num += 1
            coverage_item.covered_mapping.update(covered_mapping)

        return coverage_items

    def calculate_faults_distribution(self) -> dict[str, int]:
        """
        Calculate the distribution of faults.
        """
        faults_distribution: dict[str, int] = {}

        for item in self.count_items:
            faults_distribution[item.fault_type] = faults_distribution.get(item.fault_type, 0) + 1

        return faults_distribution

    def calculate_services_distribution(self) -> dict[str, int]:
        """
        Calculate the distribution of services.
        """
        services_distribution: dict[str, int] = {}

        for item in self.count_items:
            if not item.is_pair:
                services_distribution[item.service] = services_distribution.get(item.service, 0) + 1

        return services_distribution

    def calcuate_pairs_distribution(self) -> dict[str, PairStats]:
        """
        Calculate the distribution of service pairs.
        """
        pairs_distribution: dict[str, PairStats] = {}

        for item in self.count_items:
            if item.is_pair:
                source, target = item.service.split("->")
                if source not in pairs_distribution:
                    pairs_distribution[source] = PairStats(out_degree=1)
                else:
                    pairs_distribution[source].out_degree += 1

                if target not in pairs_distribution:
                    pairs_distribution[target] = PairStats(in_degree=1)
                else:
                    pairs_distribution[target].in_degree += 1

        return pairs_distribution

    def calculate_fault_services_distribution(self) -> dict[str, dict[str, int]]:
        fault_services_distribution: dict[str, dict[str, int]] = {}

        for item in self.count_items:
            if not item.is_pair:
                if item.fault_type not in fault_services_distribution:
                    fault_services_distribution[item.fault_type] = {}

                fault_services_distribution[item.fault_type][item.service] = (
                    fault_services_distribution[item.fault_type].get(item.service, 0) + 1
                )

        return fault_services_distribution

    def calculate_fault_service_attribute_coverages(self) -> dict[str, dict[str, float]]:
        """
        Calculate the coverage distribution of fault-service attribution.
        """
        fault_service_attribute_coverages: dict[str, dict[str, float]] = {}

        for fault_type, mapping in self.coverage_items.items():
            for mapping_key, coverage_item in mapping.items():
                if coverage_item.range_num == 0:
                    continue

                if not coverage_item.is_pair:
                    if fault_type not in fault_service_attribute_coverages:
                        fault_service_attribute_coverages[fault_type] = {}

                    ratio = len(coverage_item.covered_mapping) / coverage_item.range_num
                    fault_service_attribute_coverages[fault_type][mapping_key] = ratio

        return fault_service_attribute_coverages

    def calculate_fault_pair_attribute_coverages(self) -> dict[str, dict[str, float]]:
        """
        Calculate the coverage distribution of fault-pair attribution.
        """
        fault_pair_attribute_coverages: dict[str, dict[str, float]] = {}

        for fault_type, service_coverages in self.coverage_items.items():
            for service, coverage_item in service_coverages.items():
                if coverage_item.range_num == 0:
                    continue

                if coverage_item.is_pair:
                    if fault_type not in fault_pair_attribute_coverages:
                        fault_pair_attribute_coverages[fault_type] = {}

                    ratio = len(coverage_item.covered_mapping) / coverage_item.range_num
                    fault_pair_attribute_coverages[fault_type][service] = ratio

        return fault_pair_attribute_coverages

    def calculate_metric_distributions(self) -> dict[str, dict[str, int]]:
        """
        Calculate the distributions of metrics
        """
        metric_distributions: dict[str, dict[str, int]] = {}

        data: list[dict[str, Any]] = []
        for count_item in self.count_items:
            for key, value in count_item.metrics.items():
                data.append({"metric_name": key, "metric_value": value})

        if not data:
            return {}

        lf = pl.LazyFrame(data=data)
        metrics_filter = pl.col("metric_name").is_in(self.metrics)
        filtered_lf = lf.filter(metrics_filter)

        collected_data = filtered_lf.collect()
        if collected_data.is_empty():
            return {}

        for metric in self.metrics:
            metric_data = collected_data.filter(pl.col("metric_name") == metric)
            if metric_data.is_empty():
                continue

            try:
                numeric_data = metric_data.with_columns(
                    [pl.col("metric_value").cast(pl.Float64, strict=False).alias("numeric_value")]
                ).filter(pl.col("numeric_value").is_not_null())
                if numeric_data.is_empty():
                    continue

                distribution_stats = (
                    numeric_data.group_by("numeric_value").agg(pl.len().alias("count")).sort("numeric_value")
                )
                distribution_dict = {
                    str(row["numeric_value"]): row["count"] for row in distribution_stats.iter_rows(named=True)
                }

                metric_distributions[metric] = distribution_dict

            except Exception:
                logger.error(f"Error processing metric {metric}")

        return metric_distributions

    def calculate_fault_service_metric_distributions(self) -> dict[str, dict[str, dict[str, dict[str, int]]]]:
        """
        Calculate the distributions of fault-service metrics.
        """
        fault_service_metrics: dict[str, dict[str, dict[str, dict[str, int]]]] = {}

        data: list[dict[str, Any]] = []
        for count_item in self.count_items:
            if count_item.is_pair:
                for key, value in count_item.metrics.items():
                    data.append(
                        {
                            "fault_type": count_item.fault_type,
                            "service": count_item.service,
                            "metric_name": key,
                            "metric_value": value,
                        }
                    )

        if not data:
            return {}

        lf = pl.LazyFrame(data=data)
        metrics_filter = pl.col("metric_name").is_in(self.metrics)
        filtered_lf = lf.filter(metrics_filter)

        collected_data = filtered_lf.collect()
        if collected_data.is_empty():
            return {}

        for (fault_type_object, service_object), group_df in collected_data.group_by(["fault_type", "service"]):
            fault_type = str(fault_type_object)
            service = str(service_object)

            if fault_type not in fault_service_metrics:
                fault_service_metrics[fault_type] = {}

            if service not in fault_service_metrics[fault_type]:
                fault_service_metrics[fault_type][service] = {}

            for metric in self.metrics:
                metric_data = group_df.filter(pl.col("metric_name") == metric)

                if metric_data.is_empty():
                    continue

                distribution_stats = (
                    metric_data.group_by("metric_value").agg(pl.len().alias("count")).sort("metric_value")
                )

                distribution_dict = {
                    str(row["metric_value"]): row["count"] for row in distribution_stats.iter_rows(named=True)
                }

                fault_service_metrics[fault_type][service][metric] = distribution_dict

        return fault_service_metrics

    def get_distribution(self) -> Distribution:
        """
        Get the distribution of faults and services from the nodes.
        """
        distribution = Distribution(
            faults=self.calculate_faults_distribution(),
            services=self.calculate_services_distribution(),
            pairs=self.calcuate_pairs_distribution(),
            metrics=self.calculate_metric_distributions(),
            fault_services=self.calculate_fault_services_distribution(),
            fault_service_attribute_coverages=self.calculate_fault_service_attribute_coverages(),
            fault_service_metrics=self.calculate_fault_service_metric_distributions(),
            fault_pair_attribute_coverages=self.calculate_fault_pair_attribute_coverages(),
        )

        return distribution


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
