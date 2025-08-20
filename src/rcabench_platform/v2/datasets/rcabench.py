import json
import math
import os
import random
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import networkx as nx
import polars as pl

from ..datasets.spec import get_datapack_folder
from ..logging import logger, timeit
from ..sources.convert import link_subset
from .spec import DatasetAnalyzer, build_service_graph

DATAPACK_PATTERN = (
    r"(ts|ts\d)-(mysql|ts-rabbitmq|ts-ui-dashboard|ts-\w+-service|ts-\w+-\w+-service|ts-\w+-\w+-\w+-service)-(.+)-[^-]+"
)


def rcabench_get_service_name(datapack_name: str) -> str:
    m = re.match(DATAPACK_PATTERN, datapack_name)
    assert m is not None, f"Invalid datapack name: `{datapack_name}`"
    service_name: str = m.group(2)
    return service_name


FAULT_TYPES: list[str] = [
    "PodKill",
    "PodFailure",
    "ContainerKill",
    "MemoryStress",
    "CPUStress",
    "HTTPRequestAbort",
    "HTTPResponseAbort",
    "HTTPRequestDelay",
    "HTTPResponseDelay",
    "HTTPResponseReplaceBody",
    "HTTPResponsePatchBody",
    "HTTPRequestReplacePath",
    "HTTPRequestReplaceMethod",
    "HTTPResponseReplaceCode",
    "DNSError",
    "DNSRandom",
    "TimeSkew",
    "NetworkDelay",
    "NetworkLoss",
    "NetworkDuplicate",
    "NetworkCorrupt",
    "NetworkBandwidth",
    "NetworkPartition",
    "JVMLatency",
    "JVMReturn",
    "JVMException",
    "JVMGarbageCollector",
    "JVMCPUStress",
    "JVMMemoryStress",
    "JVMMySQLLatency",
    "JVMMySQLException",
]


def get_parent_resource_from_pod_name(
    pod_name: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Parse parent resource from Pod name (Deployment + ReplicaSet or StatefulSet/DaemonSet)

    Supported parent resource types:
    - Deployment Pods: <deployment-name>-<replicaset-hash>-<pod-hash>
        → Returns ("Deployment", deployment_name, replicaset_name)
    - StatefulSet Pods: <statefulset-name>-<ordinal>
        → Returns ("StatefulSet", statefulset_name, None)
    - DaemonSet Pods: <daemonset-name>-<pod-hash>
        → Returns ("DaemonSet", daemonset_name, None)
    - Other cases return (None, None, None)

    Args:
        podname (str): Pod name

    Returns:
        tuple: (parent_type, parent_name, replicaset_name_if_applicable)
    """
    # Deployment Pod format: <deployment-name>-<replicaset-hash>-<pod-hash>
    # Example: nginx-deployment-5c689d88bb-q7zvf
    deployment_pattern = r"^(?P<deploy>.+?)-(?P<rs_hash>[a-z0-9]{5,10})-(?P<pod_hash>[a-z0-9]{5})$"
    match = re.fullmatch(deployment_pattern, pod_name)
    if match:
        deployment_name = match.group("deploy")
        replicaset_name = f"{deployment_name}-{match.group('rs_hash')}"
        return ("Deployment", deployment_name, replicaset_name)

    # StatefulSet Pod format: <statefulset-name>-<ordinal>
    # Example: web-0, mysql-1
    statefulset_pattern = r"^(?P<sts>.+)-(\d+)$"
    match = re.fullmatch(statefulset_pattern, pod_name)
    if match:
        return ("StatefulSet", match.group("sts"), None)

    # DaemonSet Pod format: <daemonset-name>-<pod-hash>
    # Example: fluentd-elasticsearch-abcde
    daemonset_pattern = r"^(?P<ds>.+)-([a-z0-9]{5})$"
    match = re.fullmatch(daemonset_pattern, pod_name)
    if match:
        return ("DaemonSet", match.group("ds"), None)

    # Other cases (like bare Pod or unknown format)
    return (None, None, None)


HTTP_REPLACE_METHODS: list[str] = [
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "HEAD",
    "OPTIONS",
    "PATCH",
]

HTTP_REPLACE_BODY_TYPE: dict[int, str] = {
    0: "empty",
    1: "random",
}

JVM_MEM_TYPE: dict[int, str] = {
    1: "heap",
    2: "stack",
}

JVM_RETURN_TYPE: dict[int, str] = {
    1: "String",
    2: "Int",
}

JVM_RETURN_VALUE_OPT: dict[int, str] = {
    0: "Default",
    1: "Random",
}


def rcabench_fix_injection(injection: dict[str, Any]) -> None:
    injection["fault_type"] = FAULT_TYPES[injection["fault_type"]]

    injection["engine_config"] = json.loads(injection["engine_config"])

    display_config: dict[str, Any] = json.loads(injection["display_config"])
    rcabench_fix_injection_display_config(display_config)
    injection["display_config"] = display_config


def rcabench_fix_injection_display_config(display_config: dict[str, Any]) -> None:
    if (replace_method := display_config.get("replace_method")) is not None:
        if isinstance(replace_method, int):
            display_config["replace_method"] = HTTP_REPLACE_METHODS[replace_method]
        elif isinstance(replace_method, str):
            pass
        else:
            raise ValueError(f"Invalid replace_method type: {type(replace_method)}. Expected int or str.")

    replacements = [
        ("body_type", HTTP_REPLACE_BODY_TYPE),
        ("mem_type", JVM_MEM_TYPE),
        ("return_type", JVM_RETURN_TYPE),
        ("return_value_opt", JVM_RETURN_VALUE_OPT),
    ]

    for k, d in replacements:
        v = display_config.get(k)
        if v is None:
            continue
        display_config[k] = d[v]


@timeit(log_args={"train_ratio"})
def rcabench_split_train_test(
    datapacks: list[str],
    train_ratio: float,
    previous_datapacks: list[str],
    datapack_limit: int = 0,
):
    assert len(datapacks) > 0, "Datapacks list cannot be empty."
    assert 0 < train_ratio < 1, "Ratio must be between 0 and 1."
    assert datapack_limit <= len(datapacks), "Datapack limit must be less than or equal to the number of datapacks."

    prev_datapacks = set(previous_datapacks)
    additional_datapacks = set(datapacks) - prev_datapacks

    group_by_service: defaultdict[str, list[str]] = defaultdict(list)
    for datapack in additional_datapacks:
        service_name = rcabench_get_service_name(datapack)
        group_by_service[service_name].append(datapack)

    min_group_size = min(len(v) for v in group_by_service.values())
    logger.debug("min_group_size: {}", min_group_size)

    threshold = min_group_size
    while True:
        train_total = 0
        for service_datapacks in group_by_service.values():
            num_train = math.ceil(len(service_datapacks) * train_ratio)
            num_train = min(num_train, threshold)
            train_total += num_train

        target = len(additional_datapacks) * train_ratio

        logger.debug("threshold={} (train_total={}, target={})", threshold, train_total, target)

        if train_total >= target:
            break

        threshold += 1

    train_datapacks: list[str] = []
    test_datapacks: list[str] = []

    for service_name, service_datapacks in group_by_service.items():
        random.shuffle(service_datapacks)

        num_train = math.ceil(len(service_datapacks) * train_ratio)
        num_train = min(num_train, threshold)

        train_datapacks.extend(service_datapacks[:num_train])
        test_datapacks.extend(service_datapacks[num_train:])

    total_selected = len(train_datapacks) + len(test_datapacks)

    if total_selected > datapack_limit:
        target_train = int(datapack_limit * train_ratio)
        target_test = datapack_limit - target_train

        train_datapacks = train_datapacks[:target_train]
        test_datapacks = test_datapacks[:target_test]

        logger.info("Adjusted to datapack_limit: train={}, test={}", len(train_datapacks), len(test_datapacks))

    logger.info(
        "Final dataset: train={} datapacks, test={} datapacks, total={}",
        len(train_datapacks),
        len(test_datapacks),
        len(train_datapacks) + len(test_datapacks),
    )

    return train_datapacks, test_datapacks


def valid(path: Path) -> tuple[Path, bool]:
    path_obj = path

    if not path_obj.exists() or not path_obj.is_dir():
        logger.debug("Path does not exist or is not a directory: {}", path)
        return path, False

    # Check cache files first
    valid_cache = path_obj / ".valid"
    invalid_cache = path_obj / ".invalid"

    if valid_cache.exists():
        return path, True
    elif invalid_cache.exists():
        return path, False

    required_files = [
        # Parquet files
        "abnormal_logs.parquet",
        "abnormal_metrics_sum.parquet",
        "abnormal_metrics_histogram.parquet",
        "abnormal_trace_id_ts.parquet",
        "abnormal_metrics.parquet",
        "abnormal_traces.parquet",
        "normal_metrics_histogram.parquet",
        "normal_trace_id_ts.parquet",
        "normal_logs.parquet",
        "normal_metrics.parquet",
        "normal_metrics_sum.parquet",
        "normal_traces.parquet",
        # JSON files
        "injection.json",
        "k8s.json",
        "env.json",
    ]

    for filename in required_files:
        file_path = path_obj / filename

        if not file_path.exists():
            logger.debug("Missing required file: {}", file_path)
            invalid_f = path_obj / ".invalid"
            invalid_f.touch()
            return path, False

        if file_path.stat().st_size == 0:
            logger.debug("Empty file: {}", file_path)
            invalid_f = path_obj / ".invalid"
            invalid_f.touch()
            return path, False

        if filename.endswith(".json"):
            try:
                with open(file_path, encoding="utf-8") as f:
                    json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.debug("Invalid JSON file {}: {}", file_path, e)
                invalid_f = path_obj / ".invalid"
                invalid_f.touch()
                return path, False

        elif filename.endswith(".parquet"):
            try:
                df = pl.read_parquet(file_path)
                row_count = df.height

                if row_count == 0:
                    logger.debug("Parquet file has no data rows: {}", file_path)
                    invalid_f = path_obj / ".invalid"
                    invalid_f.touch()
                    return path, False

            except Exception as e:
                logger.debug("Failed to read Parquet file {}: {}", file_path, e)
                invalid_f = path_obj / ".invalid"
                invalid_f.touch()
                return path, False

    # All validation passed, create valid cache file
    valid_f = path_obj / ".valid"
    valid_f.touch()
    return path, True


class RCABenchAnalyzerLoader(DatasetAnalyzer):
    # Golden signal metrics definition (SRE four golden signals: latency, traffic, errors, saturation)

    _GOLDEN_SIGNAL_METRICS = {
        "latency": [
            "http.client.request.duration",
            "http.server.request.duration",
            "db.client.connections.use_time",
            "db.client.connections.create_time",
            "db.client.connections.wait_time",
            "jvm.gc.duration",
        ],
        "traffic": [
            "hubble_flows_processed_total",
            "processedSpans",
            "processedLogs",
            "hubble_icmp_total",
            "hubble_port_distribution_total",
            "hubble_tcp_flags_total",
            "otlp.exporter.seen",
            "otlp.exporter.exported",
            "k8s.pod.network.io",
        ],
        "error": [
            "hubble_drop_total",
            "k8s.pod.network.errors",
            "k8s.container.restarts",
        ],
        "saturation": [
            "container.cpu.usage",
            "k8s.pod.cpu.usage",
            "k8s.pod.cpu_limit_utilization",
            "k8s.pod.cpu.node.utilization",
            "jvm.cpu.recent_utilization",
            "jvm.system.cpu.utilization",
            "jvm.system.cpu.load_1m",
            "container.memory.usage",
            "k8s.pod.memory.usage",
            "k8s.pod.memory_limit_utilization",
            "k8s.pod.memory.node.utilization",
            "container.memory.working_set",
            "k8s.pod.memory.working_set",
            "jvm.memory.used",
            "container.filesystem.usage",
            "k8s.pod.filesystem.usage",
            "queueSize",
        ],
    }

    def __init__(self, datapack: str):
        super().__init__(datapack)
        self.files: dict[str, Any] = self._load_datapack_files()

    def _get_datapack_folder(self) -> Path | None:
        return Path("data") / "rcabench_dataset" / self.datapack / "converted"

    def _load_datapack_files(self) -> dict[str, Any]:
        folder = self._get_datapack_folder()
        assert folder is not None, "datapack folder must exist"
        files: dict[str, Any] = {}

        for file_type in [
            "traces",
            "logs",
            "metrics",
            "metrics_sum",
            "metrics_histogram",
        ]:
            normal_file = folder / f"normal_{file_type}.parquet"
            abnormal_file = folder / f"abnormal_{file_type}.parquet"
            if normal_file.exists():
                lf = pl.scan_parquet(normal_file)
                lf = lf.sort("time")
                files[f"normal_{file_type}"] = lf
            if abnormal_file.exists():
                lf = pl.scan_parquet(abnormal_file)
                lf = lf.sort("time")
                files[f"abnormal_{file_type}"] = lf

        for json_file in ["env.json", "injection.json"]:
            json_path = folder / json_file
            if json_path.exists():
                with open(json_path) as f:
                    files[json_file.replace(".json", "")] = json.load(f)

        conclusion_file = folder / "conclusion.parquet"
        if conclusion_file.exists():
            lf = pl.scan_parquet(conclusion_file)
            files["conclusion"] = lf

        return files

    def get_traces(self, abnormal: bool = False) -> pl.LazyFrame | None:
        key = "abnormal_traces" if abnormal else "normal_traces"
        return self.files.get(key)

    def get_metrics(self, abnormal: bool = False) -> pl.LazyFrame | None:
        key = "abnormal_metrics" if abnormal else "normal_metrics"
        return self.files.get(key)

    def get_logs(self, abnormal: bool = False) -> pl.LazyFrame | None:
        key = "abnormal_logs" if abnormal else "normal_logs"
        return self.files.get(key)

    def get_conclusion(self) -> pl.LazyFrame | None:
        return self.files.get("conclusion")

    def get_service_dependency_graph(self) -> nx.DiGraph:
        folder = self._get_datapack_folder()
        assert folder is not None, "datapack folder must exist"

        normal_traces = pl.scan_parquet(folder / "normal_traces.parquet")
        anomal_traces = pl.scan_parquet(folder / "abnormal_traces.parquet")
        traces = pl.concat([normal_traces, anomal_traces])

        return build_service_graph(traces)

    def get_all_services(self) -> list[str]:
        services = set()
        for key in [
            "normal_traces",
            "abnormal_traces",
            "normal_metrics",
            "abnormal_metrics",
        ]:
            lf = self.files.get(key)
            if lf is not None and "service_name" in lf.collect_schema():
                services.update(lf.select("service_name").unique().collect()["service_name"].to_list())

        services.discard("loadgenerator-service")
        services.discard("")
        return list(services)

    def get_service_metrics(self, service_name: str, abnormal: bool = False) -> dict[str, list[float]]:
        metrics_lf = self.get_metrics(abnormal=abnormal)
        if metrics_lf is None:
            return {}

        return self._extract_service_metrics(metrics_lf, service_name)

    def get_root_services(self) -> list[str]:
        injection = self.files.get("injection", {})
        assert isinstance(injection, dict), "injection must be a dictionary"
        if not injection:
            return []
        ground_truth = injection.get("ground_truth", {})
        root_services = ground_truth.get("service", [])
        return root_services

    def get_entry_service(self) -> str | None:
        return "loadgenerator"

    def _extract_service_metrics(self, metrics_lf: pl.LazyFrame, service_name: str) -> dict[str, list[float]]:
        assert isinstance(metrics_lf, pl.LazyFrame), "metrics_lf must be a polars LazyFrame"
        assert isinstance(service_name, str) and service_name.strip(), "service_name must be a non-empty string"

        schema = metrics_lf.collect_schema()
        assert "service_name" in schema, "metrics_lf must have service_name column"
        assert "metric" in schema, "metrics_lf must have metric column"
        assert "value" in schema, "metrics_lf must have value column"

        service_metrics = (
            metrics_lf.filter(pl.col("service_name") == service_name)
            .group_by("metric")
            .agg(pl.col("value").alias("values"))
            .collect()
        )

        metrics_dict = {}
        for row in service_metrics.iter_rows(named=True):
            metric_name = row["metric"]
            values = row["values"]
            if not self._is_golden_signal_metric(metric_name):
                continue
            for value in values:
                assert isinstance(value, (int, float)), f"metric value must be numeric, got {type(value)}"
            metrics_dict[metric_name] = values
        return metrics_dict

    def _is_golden_signal_metric(self, metric_name: str) -> bool:
        for metrics_list in self._GOLDEN_SIGNAL_METRICS.values():
            if metric_name in metrics_list:
                return True
            for metric in metrics_list:
                if metric in metric_name:
                    return True
        return False
