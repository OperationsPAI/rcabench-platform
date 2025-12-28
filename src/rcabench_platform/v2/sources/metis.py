import json
import re
from pathlib import Path
from typing import Any

import polars as pl
import tomli
from drain3 import TemplateMiner
from drain3.drain import LogCluster

from ..logging import logger, timeit
from ..utils.serde import load_json, save_json, save_parquet
from .convert import DatapackLoader, DatasetLoader, Label
from .rcabench import create_template_miner, extract_path


def convert_traces(src: Path) -> pl.DataFrame:
    # "Timestamp","TraceId","SpanId","ParentSpanId","SpanName","ServiceName","Duration","ParentServiceName"
    lf = pl.scan_csv(
        src, infer_schema_length=10000, schema_overrides={"Timestamp": pl.String, "Duration": pl.UInt64}
    ).select(
        pl.col("Timestamp").alias("time"),
        pl.col("TraceId").alias("trace_id"),
        pl.col("SpanId").alias("span_id"),
        pl.col("ParentSpanId").alias("parent_span_id"),
        pl.col("SpanName").alias("span_name"),
        pl.col("ServiceName").alias("service_name"),
        pl.col("Duration").cast(pl.UInt64).alias("duration"),
    )

    # Convert time string to datetime
    lf = lf.with_columns(pl.col("time").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False))

    # Add missing columns as nulls to match rcabench format
    lf = lf.with_columns(
        [
            pl.lit(None, dtype=pl.String).alias("attr.span_kind"),
            pl.lit(None, dtype=pl.String).alias("attr.status_code"),
            pl.lit(None, dtype=pl.String).alias("attr.k8s.pod.name"),
            pl.lit(None, dtype=pl.String).alias("attr.k8s.service.name"),
            pl.lit(None, dtype=pl.String).alias("attr.k8s.namespace.name"),
            pl.lit(None, dtype=pl.String).alias("attr.http.request.method"),
            pl.lit(None, dtype=pl.UInt16).alias("attr.http.response.status_code"),
            pl.lit(None, dtype=pl.UInt64).alias("attr.http.request.content_length"),
            pl.lit(None, dtype=pl.UInt64).alias("attr.http.response.content_length"),
        ]
    )

    return lf.collect().with_columns(
        [
            pl.when(pl.col("service_name").is_in(["loadgenerator", "ts-ui-dashboard"]))
            .then(pl.col("span_name").map_elements(extract_path, return_dtype=pl.String))
            .otherwise(pl.col("span_name"))
            .alias("span_name")
        ]
    )


def convert_metrics(src: Path) -> pl.DataFrame:
    # "k8s_namespace_name","k8s_pod_uid","k8s_pod_name","k8s_container_name","MetricName","MetricDescription","TimeUnix","Value","MetricUnit","direction"
    lf = pl.scan_csv(
        src, infer_schema_length=10000, schema_overrides={"TimeUnix": pl.String, "Value": pl.Float64}
    ).select(
        pl.col("TimeUnix").alias("time"),
        pl.col("MetricName").alias("metric"),
        pl.col("Value").cast(pl.Float64).alias("value"),
        pl.col("k8s_pod_name").alias("attr.k8s.pod.name"),
        pl.col("k8s_container_name").alias("service_name"),
        pl.col("k8s_namespace_name").alias("attr.k8s.namespace.name"),
    )

    # Convert time string to datetime
    lf = lf.with_columns(pl.col("time").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False))

    # If service_name is empty, extract it from pod_name
    # Deployment Pod format: <deployment-name>-<replicaset-hash>-<pod-hash>
    deployment_pattern = r"^(?P<deploy>.+?)-[a-z0-9]{5,10}-[a-z0-9]{5}$"

    def extract_service(pod_name: Any, service_name: Any) -> str:
        if service_name and isinstance(service_name, str) and service_name != "":
            return service_name
        if pod_name and isinstance(pod_name, str):
            match = re.match(deployment_pattern, pod_name)
            if match:
                return match.group("deploy")
            return pod_name
        return ""

    lf = lf.with_columns(
        pl.struct([pl.col("attr.k8s.pod.name").alias("pod_name"), "service_name"])
        .map_elements(lambda x: extract_service(x["pod_name"], x["service_name"]), return_dtype=pl.String)
        .alias("service_name")
    )

    return lf.collect()


def convert_logs(src: Path) -> pl.DataFrame:
    # "Timestamp","TraceId","SpanId","SeverityText","SeverityNumber","ServiceName","Body"
    lf = (
        pl.scan_csv(src, infer_schema_length=10000, schema_overrides={"Timestamp": pl.String})
        .select(
            pl.col("Timestamp").alias("time"),
            pl.col("TraceId").alias("trace_id"),
            pl.col("SpanId").alias("span_id"),
            pl.col("SeverityText").alias("level"),
            pl.col("ServiceName").alias("service_name"),
            pl.col("Body").alias("message"),
        )
        .filter(pl.col("service_name") != "ts-ui-dashboard")
    )

    # Convert time string to datetime
    lf = lf.with_columns(pl.col("time").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False))

    df = lf.collect()

    # Extract unique messages for template processing
    unique_messages = df.select("message").unique()

    if unique_messages.height > 0:
        # Determine template paths
        # Fallback to a known location if not provided

        template_base = src.parent.parent.parent.parent / "drain_template"

        config_path = template_base / "drain_ts.ini"
        persistence_path = template_base / "drain_ts.bin"

        logger.info(f"Using template paths: config={config_path}, persistence={persistence_path}")

        template_miner = create_template_miner(config_path, persistence_path)

        message_mappings = []
        for message in unique_messages["message"].to_list():
            if message:  # Skip empty messages
                result = template_miner.add_log_message(message)
                template_id = result["cluster_id"]
                cluster = template_miner.drain.id_to_cluster.get(template_id)
                if isinstance(cluster, LogCluster):
                    log_template = cluster.get_template()
                else:
                    log_template = ""
                message_mappings.append(
                    {
                        "message": message,
                        "attr.log_template": log_template,
                        "attr.template_id": str(template_id),
                    }
                )
            else:
                message_mappings.append(
                    {
                        "message": message,
                        "attr.log_template": "",
                        "attr.template_id": "",
                    }
                )

        mapping_df = pl.DataFrame(message_mappings)
        df = df.join(mapping_df, on="message", how="left")

    else:
        df = df.with_columns(
            [
                pl.lit("", dtype=pl.String).alias("attr.log_template"),
                pl.lit("", dtype=pl.String).alias("attr.template_id"),
            ]
        )

    return df


class MetisDatapackLoader(DatapackLoader):
    def __init__(self, src_folder: Path, datapack: str, injection_info: dict[str, Any]) -> None:
        self._src_folder = src_folder
        self._datapack = datapack
        self._injection_info = injection_info

    def name(self) -> str:
        return self._datapack

    def labels(self) -> list[Label]:
        service = self._injection_info.get("service")
        if service:
            return [Label(level="service", name=service)]
        return []

    def data(self) -> dict[str, Any]:
        ans: dict[str, Any] = {}

        # Handle traces, metrics, logs
        converters = {
            "traces.csv": ("_traces.parquet", convert_traces),
            "metrics.csv": ("_metrics.parquet", convert_metrics),
            "logs.csv": ("_logs.parquet", convert_logs),
        }

        all_dfs = {}
        for prefix in ("normal", "abnormal"):
            for csv_name, (suffix, func) in converters.items():
                csv_path = self._src_folder / prefix / csv_name
                if csv_path.exists():
                    df = func(csv_path)
                    ans[f"{prefix}{suffix}"] = df
                    all_dfs[f"{prefix}_{csv_name}"] = df

        # Calculate time ranges for env.json
        normal_traces = all_dfs.get("normal_traces.csv")
        abnormal_traces = all_dfs.get("abnormal_traces.csv")

        if normal_traces is not None and abnormal_traces is not None:
            normal_start = normal_traces["time"].min()
            normal_end = normal_traces["time"].max()
            abnormal_start = abnormal_traces["time"].min()
            abnormal_end = abnormal_traces["time"].max()

            ans["env.json"] = {
                "NORMAL_START": int(normal_start.timestamp()),
                "NORMAL_END": int(normal_end.timestamp()),
                "ABNORMAL_START": int(abnormal_start.timestamp()),
                "ABNORMAL_END": int(abnormal_end.timestamp()),
                "TIMEZONE": "UTC",
            }

        # Create injection.json
        ans["injection.json"] = {
            "fault_type": self._injection_info.get("chaos_type", "Unknown"),
            "display_config": json.dumps(
                {
                    "service": self._injection_info.get("service"),
                    "timestamp": (
                        self._injection_info.get("timestamp").isoformat()
                        if hasattr(self._injection_info.get("timestamp"), "isoformat")
                        else self._injection_info.get("timestamp")
                    ),
                }
            ),
            "ground_truth": {
                "service": [self._injection_info.get("service")] if self._injection_info.get("service") else []
            },
        }

        return ans


class MetisDatasetLoader(DatasetLoader):
    def __init__(self, src_root: Path, sub_folder: str = "ts", dataset: str = "metis-ts") -> None:
        self._src_root = src_root
        self._sub_folder = sub_folder
        self._dataset = dataset
        self._injection_map = self._load_injection_info()
        self._datapacks = sorted(list(self._injection_map.keys()))

    def _load_injection_info(self) -> dict[str, Any]:
        toml_path = self._src_root / self._sub_folder / "fault_injection.toml"
        if not toml_path.exists():
            logger.warning(f"Injection info not found at {toml_path}")
            return {}

        with open(toml_path, "rb") as f:
            data = tomli.load(f)

        return {item["case"]: item for item in data.get("chaos_injection", [])}

    def name(self) -> str:
        return self._dataset

    def __len__(self) -> int:
        return len(self._datapacks)

    def __getitem__(self, index: int) -> DatapackLoader:
        datapack = self._datapacks[index]
        return MetisDatapackLoader(
            src_folder=self._src_root / self._sub_folder / datapack,
            datapack=datapack,
            injection_info=self._injection_map[datapack],
        )
