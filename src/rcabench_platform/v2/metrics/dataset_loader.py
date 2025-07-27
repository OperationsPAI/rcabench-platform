import json
from typing import Any, Union

import networkx as nx
import polars as pl

from ..datasets.spec import get_datapack_folder


class DatasetLoader:
    def __init__(self, dataset: str, datapack: str):
        assert isinstance(dataset, str) and dataset.strip(), "dataset must be a non-empty string"
        assert isinstance(datapack, str) and datapack.strip(), "datapack must be a non-empty string"
        self.dataset: str = dataset
        self.datapack: str = datapack
        self.files: dict[str, Any] = self._load_datapack_files()

    def _load_datapack_files(self) -> dict[str, Any]:
        folder = get_datapack_folder(self.dataset, self.datapack)
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

        for json_file in ["injection.json", "env.json", "notations.json"]:
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
        """Get logs data sorted by timestamp"""
        key = "abnormal_logs" if abnormal else "normal_logs"
        return self.files.get(key)

    def get_conclusion(self) -> pl.LazyFrame | None:
        return self.files.get("conclusion")

    def get_service_dependency_graph(self) -> nx.Graph:
        normal_traces = self.get_traces(abnormal=False)
        abnormal_traces = self.get_traces(abnormal=True)
        return _build_service_dependency_graph(normal_traces, abnormal_traces)

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

        return _extract_service_metrics(metrics_lf, service_name)

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


def _build_service_dependency_graph(
    normal_traces: pl.LazyFrame | None = None,
    abnormal_traces: pl.LazyFrame | None = None,
) -> nx.Graph:
    assert normal_traces is not None or abnormal_traces is not None, "At least one traces dataset must be provided"
    traces_lfs = []
    if normal_traces is not None:
        traces_lfs.append(normal_traces)
    if abnormal_traces is not None:
        traces_lfs.append(abnormal_traces)
    traces_lf = pl.concat(traces_lfs, how="diagonal") if len(traces_lfs) > 1 else traces_lfs[0]

    graph = nx.Graph()
    all_services = traces_lf.select("service_name").unique().filter(pl.col("service_name").is_not_null()).collect()
    service_list = all_services["service_name"].to_list()
    for service in service_list:
        graph.add_node(service)

    span_to_service = (
        traces_lf.filter(pl.col("span_id").is_not_null() & pl.col("service_name").is_not_null())
        .select(["span_id", "service_name"])
        .unique()
        .collect()
    )
    span_ids = span_to_service["span_id"].to_list()
    service_names = span_to_service["service_name"].to_list()
    span_service_map = dict(zip(span_ids, service_names))

    parent_child_relations = (
        traces_lf.filter(pl.col("parent_span_id").is_not_null() & pl.col("service_name").is_not_null())
        .select(["parent_span_id", "service_name"])
        .unique()
        .collect()
    )
    for parent_span_id, child_service in parent_child_relations.iter_rows():
        if parent_span_id in span_service_map:
            parent_service = span_service_map[parent_span_id]
            if parent_service != child_service and parent_service and child_service:
                graph.add_edge(parent_service, child_service)
    return graph


def _extract_service_metrics(metrics_lf: pl.LazyFrame, service_name: str) -> dict[str, list[float]]:
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
        if not _is_golden_signal_metric(metric_name):
            continue
        for value in values:
            assert isinstance(value, (int, float)), f"metric value must be numeric, got {type(value)}"
        metrics_dict[metric_name] = values
    return metrics_dict


def _is_golden_signal_metric(metric_name: str) -> bool:
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
    for metrics_list in _GOLDEN_SIGNAL_METRICS.values():
        if metric_name in metrics_list:
            return True
        for metric in metrics_list:
            if metric in metric_name:
                return True
    return False
