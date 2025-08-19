import functools
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import polars as pl
from rcabench.openapi import (
    DtoGranularityRecord,
    DtoInjectionFieldMappingResp,
    DtoInjectionV2Response,
    HandlerNode,
    HandlerResources,
    InjectionApi,
)

from ..clients.rcabench_ import RCABenchClient
from ..datasets.spec import calculate_trace_length
from ..logging import logger
from ..utils.fmap import fmap_processpool


@dataclass
class Item:
    _node: HandlerNode
    _injection: DtoInjectionV2Response
    fault_type: str = ""
    injected_service: str = ""
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


def get_conf(namespace: str, base_url: str | None = None) -> HandlerNode:
    with RCABenchClient(base_url=base_url) as client:
        injector = InjectionApi(client)
        resp = injector.api_v1_injections_conf_get(namespace=namespace)
        assert resp.data is not None
        return resp.data


def get_resources(namespace: str) -> tuple[DtoInjectionFieldMappingResp, HandlerResources]:
    with RCABenchClient() as client:
        injector = InjectionApi(client)

        resp = injector.api_v1_injections_mapping_get()
        assert resp.data is not None
        mapping_data = resp.data

        resp = injector.api_v1_injections_ns_resources_get(namespace=namespace)
        assert resp.data is not None
        resources_data = resp.data

        return mapping_data, resources_data


def get_individual_service(
    individual: HandlerNode,
    injection_mapping: DtoInjectionFieldMappingResp,
    injection_resources: HandlerResources,
) -> tuple[str, bool]:
    fault_type_index = str(individual.value)

    assert injection_mapping.fault_type is not None
    assert injection_mapping.fault_resource is not None
    assert individual.children is not None

    fault_type: str = injection_mapping.fault_type[fault_type_index]
    fault_resource_meta: dict[str, Any] = injection_mapping.fault_resource[fault_type]
    fault_resource_name: str = fault_resource_meta["name"]
    fault_resource = injection_resources.to_dict().get(fault_resource_name)

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


def process_item(
    injection: DtoInjectionV2Response,
    metrics: list[str],
    injection_mapping: DtoInjectionFieldMappingResp,
    injection_resources: HandlerResources,
) -> Item | None:
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
    assert injection_mapping.fault_type is not None, "Fault type mapping must not be None"
    fault_type = injection_mapping.fault_type[str(fault)]
    service, is_pair = get_individual_service(node, injection_mapping, injection_resources)

    assert injection.labels is not None
    tags = [label.value for label in injection.labels if label.key == "tag"]
    label_mapping = {label.key: label.value for label in injection.labels if label.key}

    metric_values: dict[str, int] = {}

    for metric in metrics:
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
    #     for algorithm in algorithms:
    #         resp = evaluator.api_v2_evaluations_algorithms_algorithm_datapacks_datapack_get(
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
    depth_results = calculate_trace_length(trace_spans)
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

    return Item(
        _node=node,
        _injection=injection,
        fault_type=fault_type,
        injected_service=service,
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


def batch_process_item(injections: list[DtoInjectionV2Response], metrics: list[str], namespace: str) -> list[Item]:
    injection_mapping, injection_resources = get_resources(namespace)

    tasks = []
    for injection in injections:
        tasks.append(functools.partial(process_item, injection, metrics, injection_mapping, injection_resources))

    cpu = os.cpu_count()
    assert cpu is not None

    results = fmap_processpool(tasks, parallel=cpu // 2, cpu_limit_each=2)

    return [i for i in results if i is not None]
