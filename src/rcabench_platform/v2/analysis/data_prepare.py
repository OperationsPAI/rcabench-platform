import functools
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import polars as pl
from rcabench.openapi import (
    DatasetsApi,
    DtoAlgorithmDatapackReq,
    DtoAlgorithmDatasetResp,
    DtoDatapackEvaluationBatchReq,
    DtoDatapackEvaluationItem,
    DtoGranularityRecord,
    DtoInjectionFieldMappingResp,
    DtoInjectionV2Response,
    DtoInjectionV2SearchReq,
    EvaluationApi,
    HandlerGroundtruth,
    HandlerNode,
    HandlerResources,
    InjectionApi,
    InjectionsApi,
    ProjectsApi,
)

from ..clients.rcabench_ import RCABenchClient, get_datapacks_from_dataset_id, get_evaluation_by_dataset
from ..datasets.spec import calculate_trace_length
from ..logging import logger
from ..metrics.algo_metrics import AlgoMetricItem, calculate_metrics_for_level
from ..utils.env import debug, getenv_int
from ..utils.fmap import fmap_processpool
from ..utils.fs import has_recent_file
from ..utils.profiler import global_profiler, print_profiler_stats
from ..utils.serde import load_pickle, save_pickle

if debug():
    _DEFAULT_ITEMS_CACHE_TIME = 600
else:
    _DEFAULT_ITEMS_CACHE_TIME = 0

ITEMS_CACHE_TIME = getenv_int("ITEMS_CACHE_TIME", default=_DEFAULT_ITEMS_CACHE_TIME)


@dataclass
class InputItem:
    injection: DtoInjectionV2Response
    algo_durations: dict[str, float]  # algorithm -> execution_duration
    algo_evals: dict[str, tuple[HandlerGroundtruth, list[DtoGranularityRecord]]] | None = None


@dataclass
class Item:
    # Required fields (no default values)
    _injection: DtoInjectionV2Response
    _node: HandlerNode

    # Optional fields with default values
    fault_type: str = ""
    injected_service: str = ""
    is_pair: bool = False
    anomaly_degree: Literal["absolute", "may", "no"] = "no"
    workload: Literal["trainticket"] = "trainticket"

    # Algo Metric statistics  TODO: @Lincyaw @rainysteven1 add execution time of the algo
    _algo_evals: dict[str, tuple[HandlerGroundtruth, list[DtoGranularityRecord]]] | None = None
    _algo_durations: dict[str, float] = field(default_factory=dict)
    algo_metrics: dict[str, AlgoMetricItem] = field(default_factory=dict)

    # Data statistics
    duration: timedelta = timedelta(seconds=0)  # duration in seconds
    trace_count: int = 0  # number of traces
    service_names: set[str] = field(default_factory=set)
    service_names_by_trace: set[str] = field(default_factory=set)  # trace

    # Datapack Metric statistics
    datapack_metric_values: dict[str, int] = field(default_factory=dict)  # metric_name -> value

    # Injection Metric statistics
    injection_metric_counts: dict[str, int] = field(default_factory=dict)  # metric_name -> count

    # Log statistics
    log_lines: dict[str, int] = field(default_factory=dict)  # service_name -> log_lines

    # Trace depth statistics
    trace_length: Counter[int] = field(default_factory=Counter)

    def __post_init__(self):
        if self._algo_evals is None:
            self._algo_evals = {}
            return

        self.algo_metrics = {}
        for algo, (groundtruth, predictions) in self._algo_evals.items():
            assert groundtruth.service is not None
            metric_item = calculate_metrics_for_level(
                groundtruth_items=[groundtruth.service[0]], predictions=predictions, level="service"
            )

            if algo in self._algo_durations:
                metric_item.time = self._algo_durations[algo]
            self.algo_metrics[algo] = metric_item

    @property
    def node(self) -> HandlerNode:
        return self._node

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


def get_conf(namespace: str) -> HandlerNode:
    with RCABenchClient() as client:
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


def get_execution_item(
    algorithms: list[str],
    dataset_id: int,
    execution_tag: str | None = None,
) -> tuple[list[InputItem], list[tuple[str, str]]]:
    """
    Retrieve execution items for given algorithms and dataset.

    Args:
        algorithms: List of algorithm names to process
        dataset_id: Dataset identifier
        execution_tag: Tag for the execution

    Returns:
        Tuple of (input_items, run_status_map)
    """
    unrunned_algo: list[tuple[str, str]] = []
    input_items: list[InputItem] = []

    # Get dataset information
    datapack_infos, dataset, dataset_version = get_datapacks_from_dataset_id(dataset_id)

    # Build algorithm-datapack execution mapping more efficiently
    algorithm_executions: dict[str, list[DtoDatapackEvaluationItem]] = {}

    all_possible_executions = {
        (algo, dp.injection_name) for algo in algorithms for dp in datapack_infos if dp.injection_name is not None
    }
    executed_pairs: set[tuple[str, str]] = set()

    for algorithm in algorithms:
        evaluations = get_evaluation_by_dataset(algorithm, dataset, dataset_version, execution_tag)

        for evaluation in evaluations:
            assert evaluation.items is not None, f"Evaluation items are None for algorithm {algorithm}"
            assert evaluation.algorithm is not None, "Algorithm is None in evaluation"

            if evaluation.algorithm not in algorithm_executions:
                algorithm_executions[evaluation.algorithm] = []
            algorithm_executions[evaluation.algorithm].extend(evaluation.items)

            for item in evaluation.items:
                if item.datapack_name:
                    executed_pairs.add((evaluation.algorithm, item.datapack_name))

    unrunned_algo = sorted(list(all_possible_executions - executed_pairs))

    # Process each datapack and build input items
    for datapack in datapack_infos:
        algo_durations: dict[str, float] = {}
        algo_evaluations: dict[str, tuple[HandlerGroundtruth, list[DtoGranularityRecord]]] = {}

        # Match evaluations to current datapack
        for algorithm, executions in algorithm_executions.items():
            for execution in executions:
                if execution.datapack_name == datapack.injection_name:
                    assert execution.predictions is not None, f"Predictions are None for {algorithm}"
                    assert execution.groundtruth is not None, f"Groundtruth is None for {algorithm}"
                    assert execution.execution_duration is not None, f"Execution duration is None for {algorithm}"

                    algo_evaluations[algorithm] = (execution.groundtruth, execution.predictions)
                    algo_durations[algorithm] = float(execution.execution_duration)

        # Only create InputItem if we have data for this datapack
        if algo_evaluations or algo_durations:
            input_item = InputItem(injection=datapack, algo_durations=algo_durations, algo_evals=algo_evaluations)
            input_items.append(input_item)

    return input_items, unrunned_algo


def process_item(
    algo_evals: dict[str, tuple[HandlerGroundtruth, list[DtoGranularityRecord]]] | None,
    algo_durations: dict[str, float],
    injection: DtoInjectionV2Response,
    injection_mapping: DtoInjectionFieldMappingResp,
    injection_resources: HandlerResources,
    metrics: list[str],
) -> Item | None:
    profiler = global_profiler

    if not injection.engine_config or not injection.injection_name:
        return None

    datapack_path = Path("data/rcabench_dataset") / injection.injection_name / "converted"
    with profiler.profile("prepare"):
        node = HandlerNode.from_json(str(injection.engine_config))
        fault = node.value
        assert fault is not None, "Node value must not be None"
        assert injection_mapping.fault_type is not None, "Fault type mapping must not be None"
        fault_type = injection_mapping.fault_type[str(fault)]
        service, is_pair = get_individual_service(node, injection_mapping, injection_resources)

        service_names: set[str] = set()
        service_names_by_trace: set[str] = set()
        trace_length: Counter[int] = Counter()
        duration: timedelta = timedelta(seconds=0)
        trace_count: int = 0

        assert injection.labels is not None
        tags = [label.value for label in injection.labels if label.key == "tag" and label.value]
        label_mapping = {label.key: label.value for label in injection.labels if label.key and label.value}

        datapack_metric_values: dict[str, int] = {}
        for metric in metrics:
            value = 0
            value_str = label_mapping.get(metric)
            if value_str is not None:
                value = int(value_str)

            datapack_metric_values[metric] = value

        metric_df = pl.concat(
            [
                pl.scan_parquet(datapack_path / "normal_metrics.parquet"),
                pl.scan_parquet(datapack_path / "abnormal_metrics.parquet"),
                pl.scan_parquet(datapack_path / "normal_metrics_sum.parquet"),
                pl.scan_parquet(datapack_path / "abnormal_metrics_sum.parquet"),
            ]
        )
    with profiler.profile("scan_metric"):
        service_names.update(set(metric_df.select("service_name").unique().collect().to_series().to_list()))

        metric_count_df = metric_df.select("metric").collect()
        injection_metric_counts: dict[str, int] = dict(
            metric_count_df.group_by("metric").agg(pl.len().alias("count")).iter_rows()
        )
    with profiler.profile("scan_trace"):
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

    with profiler.profile("scan_log"):
        log_df = pl.concat(
            [
                pl.scan_parquet(datapack_path / "normal_logs.parquet"),
                pl.scan_parquet(datapack_path / "abnormal_logs.parquet"),
            ]
        )

        log_service_counts = log_df.group_by("service_name").agg(pl.len().alias("count")).collect()
        log_lines: dict[str, int] = {
            row["service_name"]: row["count"] for row in log_service_counts.iter_rows(named=True)
        }
        log_service_names = set(log_df.select("service_name").unique().collect().to_series().to_list())
        service_names.update(log_service_names)
        service_names.remove("")

        anomaly_degree = "no"
        if "absolute_anomaly" in tags:
            anomaly_degree = "absolute"
        elif "may_anomaly" in tags:
            anomaly_degree = "may"

    return Item(
        _algo_evals=algo_evals,
        _algo_durations=algo_durations,
        _injection=injection,
        _node=node,
        fault_type=fault_type,
        injected_service=service,
        is_pair=is_pair,
        anomaly_degree=anomaly_degree,
        duration=duration,
        trace_count=trace_count,
        service_names=service_names,
        service_names_by_trace=service_names_by_trace,
        log_lines=log_lines,
        datapack_metric_values=datapack_metric_values,
        injection_metric_counts=injection_metric_counts,
        trace_length=trace_length,
    )


def batch_process_item(
    input_items: list[InputItem],
    metrics: list[str],
    namespace: str,
) -> list[Item]:
    injection_mapping, injection_resources = get_resources(namespace)

    tasks = [
        functools.partial(
            process_item,
            input_item.algo_evals,
            input_item.algo_durations,
            input_item.injection,
            injection_mapping,
            injection_resources,
            metrics,
        )
        for input_item in input_items
    ]

    cpu = os.cpu_count()
    assert cpu is not None, "CPU count must not be None"
    res = fmap_processpool(tasks, parallel=cpu // 2, cpu_limit_each=2)

    filtered_results = [i for i in res if i is not None]

    print_profiler_stats()
    return filtered_results


def build_items_with_cache(
    output_pkl_path: Path,
    input_items: list[InputItem],
    metrics: list[str],
    namespace: str,
) -> list[Item]:
    if not output_pkl_path.parent.exists():
        output_pkl_path.parent.mkdir(parents=True, exist_ok=True)

    # if has_recent_file(output_pkl_path, seconds=3600):
    #     return load_pickle(path=output_pkl_path)

    items = batch_process_item(input_items, metrics, namespace)

    save_pickle(items, path=output_pkl_path)

    return items
