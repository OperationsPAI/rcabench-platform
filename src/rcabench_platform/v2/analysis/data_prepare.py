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
    DtoDatapackEvaluationBatchReq,
    DtoGranularityRecord,
    DtoInjectionFieldMappingResp,
    DtoInjectionV2Response,
    DtoInjectionV2SearchReq,
    EvaluationApi,
    HandlerNode,
    HandlerResources,
    InjectionApi,
    InjectionsApi,
    ProjectsApi,
)

from ..clients.rcabench_ import RCABenchClient
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
    algo_evals: dict[str, Any] | None = None


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
    _algo_evals: dict[str, list[DtoGranularityRecord]] | None = None
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

        self.algo_metrics = {
            algo: calculate_metrics_for_level(
                groundtruth_items=[self.injected_service], predictions=predictions, level="service"
            )
            for algo, predictions in self._algo_evals.items()
        }

    @property
    def node(self) -> HandlerNode:
        """Return the HandlerNode instance associated with this item."""
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


def prepare_injections_data(
    dataset_id: int | None = None,
    project_id: int | None = None,
    abnormal_degree=["absolute_anomaly", "may_anomaly", "no_anomaly"],
) -> tuple[dict[str, list[DtoInjectionV2Response]], str]:
    with RCABenchClient() as client:

        def _get_injections() -> tuple[dict[str, list[DtoInjectionV2Response]], str]:
            api = InjectionsApi(client)

            injections_dict: dict[str, list[DtoInjectionV2Response]] = {}
            for degree in abnormal_degree:
                resp = api.api_v2_injections_search_post(
                    search=DtoInjectionV2SearchReq(
                        tags=[degree],
                        include_labels=True,
                    )
                )
                if not resp or not resp.data or not resp.data.items:
                    raise ValueError(f"No injections found for degree {degree}")

                injections_dict[degree] = resp.data.items

            return injections_dict, "injections"

        def _get_injections_by_id() -> tuple[dict[str, list[DtoInjectionV2Response]], str]:
            injections: list[DtoInjectionV2Response] = []
            folder_name = ""

            if dataset_id is not None:
                api = DatasetsApi(client)
                resp = api.api_v2_datasets_id_get(id=dataset_id, include_injections=True)

                if not resp or not resp.data or not resp.data.injections:
                    raise ValueError(f"No injections found for dataset {dataset_id}")

                injections = resp.data.injections
                folder_name = f"dataset_{dataset_id}"

            elif project_id is not None:
                api = ProjectsApi(client)
                resp = api.api_v2_projects_id_get(id=project_id, include_injections=True)

                if not resp or not resp.data or not resp.data.injections:
                    raise ValueError(f"No injections found for project {project_id}")

                injections = resp.data.injections
                folder_name = f"project_{dataset_id}"

            else:
                raise ValueError("Either dataset_id or project_id must be provided")

            items_dict: dict[str, list[DtoInjectionV2Response]] = dict([(degree, []) for degree in abnormal_degree])
            for injection in injections:
                if injection.labels is not None:
                    for label in injection.labels:
                        if label.value is not None and label.value in items_dict:
                            items_dict[label.value].append(injection)

            return items_dict, folder_name

        if dataset_id is not None or project_id is not None:
            return _get_injections_by_id()
        else:
            return _get_injections()


def get_execution_item(
    algorithms: list[str],
    dataset_id: int | None = None,
    project_id: int | None = None,
    abnormal_degree=["absolute_anomaly", "may_anomaly", "no_anomaly"],
) -> tuple[dict[str, list[InputItem]], dict[str, list[tuple[str, str]]]]:
    withDatasetID = dataset_id is not None
    withProjectID = project_id is not None

    if withDatasetID and withProjectID:
        raise ValueError("Please provide either dataset_id or project_id, not both.")

    injections_dict: dict[str, list[DtoInjectionV2Response]] = {}

    run_status_map: dict[str, list[tuple[str, str]]] = {}
    input_items: dict[str, list[InputItem]] = {}

    with RCABenchClient() as client:
        evaluator = EvaluationApi(client)

        injections_dict, _ = prepare_injections_data(
            dataset_id=dataset_id, project_id=project_id, abnormal_degree=abnormal_degree
        )

        for degree, injections in injections_dict.items():
            run_status_map[degree] = []
            input_items[degree] = []

            algo_evals: dict[str, list[DtoGranularityRecord]] = {}
            ori_df = pl.DataFrame(
                data=[
                    {"algorithm": algorithm, "datapack": injection.injection_name}
                    for algorithm in algorithms
                    for injection in injections
                ]
            )

            resp = evaluator.api_v2_evaluations_datapacks_post(
                request=DtoDatapackEvaluationBatchReq(
                    items=[
                        DtoAlgorithmDatapackReq(
                            algorithm=algorithm,
                            datapack=datapack,
                        )
                        for algorithm, datapack in ori_df.iter_rows()
                    ]
                )
            )

            assert resp.data is not None, "Failed to get evaluation data"
            eval_df = pl.DataFrame(data=resp.data)

            joined_df = ori_df.join(
                eval_df, left_on=["algorithm", "datapack"], right_on=["algorithm", "datapack"], how="inner"
            )

            injections_mapping: dict[str, DtoInjectionV2Response] = {
                injection.injection_name: injection for injection in injections if injection.injection_name is not None
            }

            for keys, group_df in joined_df.group_by("datapack"):
                datapack = str(keys[0])
                injection = injections_mapping.get(datapack)
                if injection is None:
                    logger.warning(f"No injection found for datapack {datapack}")
                    continue

                algo_evals: dict[str, list[DtoGranularityRecord]] = {}

                for row in group_df.iter_rows(named=True):
                    algorithm: str = row["algorithm"]
                    predictions = row.get("predictions", [])
                    if not predictions:
                        run_status_map[degree].append((datapack, algorithm))
                        continue

                    algo_evals[algorithm] = [DtoGranularityRecord.from_dict(p) for p in predictions]

                input_items[degree].append(
                    InputItem(
                        algo_evals=algo_evals if algo_evals else None,
                        injection=injection,
                    )
                )

    return input_items, run_status_map


def process_item(
    algo_evals: dict[str, list[DtoGranularityRecord]] | None,
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

    if has_recent_file(output_pkl_path, seconds=3600):
        return load_pickle(path=output_pkl_path)

    items = batch_process_item(input_items, metrics, namespace)

    save_pickle(items, path=output_pkl_path)

    return items
