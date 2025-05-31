#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.datasets.rcabench_ import rcabench_get_service_name, FAULT_TYPES
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.spec.convert import (
    DatasetLoader,
    DatapackLoader,
    Label,
    convert_datapack,
    convert_dataset,
    postprocess_collect_schema,
)
from rcabench_platform.v1.spec.data import META_ROOT, get_datapack_list
from rcabench_platform.v1.utils.fmap import fmap_threadpool
from rcabench_platform.v1.utils.serde import load_json, save_json, save_parquet

from pathlib import Path
from typing import Any
import functools
import datetime
import json

import polars as pl
from tqdm.auto import tqdm
import dateutil.tz


def local_to_utc(col: pl.Expr) -> pl.Expr:
    col = col.dt.replace_time_zone("Asia/Shanghai")
    return col.dt.cast_time_unit("ns").dt.convert_time_zone("UTC")


def replace_time_col(lf: pl.LazyFrame, col_name: str) -> pl.LazyFrame:
    lf = lf.with_columns(local_to_utc(pl.col(col_name))).rename({col_name: "time"})
    return lf


def unnest_json_col(lf: pl.LazyFrame, col_name: str, dtype: pl.Struct) -> pl.LazyFrame:
    lf = lf.with_columns(pl.col(col_name).str.json_decode(dtype).struct.unnest()).drop(col_name)
    return lf


def convert_metrics(src: Path) -> pl.LazyFrame:
    lf = pl.scan_parquet(src).select(
        "TimeUnix",
        "MetricName",
        "Value",
        "ResourceAttributes",
    )

    lf = replace_time_col(lf, "TimeUnix")

    lf = lf.rename({"MetricName": "metric", "Value": "value"})

    lf = lf.with_columns(pl.lit(None, dtype=pl.String).alias("service_name"))

    resource_attributes = pl.Struct(
        [
            pl.Field("k8s.node.name", pl.String),
            pl.Field("k8s.namespace.name", pl.String),
            pl.Field("k8s.statefulset.name", pl.String),
            pl.Field("k8s.deployment.name", pl.String),
            pl.Field("k8s.replicaset.name", pl.String),
            pl.Field("k8s.pod.name", pl.String),
            pl.Field("k8s.container.name", pl.String),
        ]
    )
    lf = unnest_json_col(lf, "ResourceAttributes", resource_attributes)

    attr_cols = [field.name for field in resource_attributes.fields]
    lf = lf.rename({col: "attr." + col for col in attr_cols})

    lf = lf.sort("time")

    return lf


def convert_metrics_histogram(src: Path) -> pl.LazyFrame:
    lf = pl.scan_parquet(src).select(
        "TimeUnix",
        "MetricName",
        "ServiceName",
        "ResourceAttributes",
        "Attributes",
        "Count",
        "Sum",
        # "BucketCounts",
        # "ExplicitBounds",
        "Min",
        "Max",
        # "AggregationTemporality",
    )

    lf = replace_time_col(lf, "TimeUnix")

    lf = lf.rename(
        {
            "MetricName": "metric",
            "ServiceName": "service_name",
            "Count": "count",
            "Sum": "sum",
            "Min": "min",
            "Max": "max",
        }
    )

    lf = lf.with_columns(
        pl.col("count").cast(pl.Float64),
        pl.col("sum").cast(pl.Float64),
        pl.col("min").cast(pl.Float64),
        pl.col("max").cast(pl.Float64),
    )

    resource_attributes = pl.Struct(
        [
            pl.Field("pod.name", pl.String),
            pl.Field("service.name", pl.String),
            pl.Field("service.namespace", pl.String),
        ]
    )
    lf = unnest_json_col(lf, "ResourceAttributes", resource_attributes)
    lf = lf.rename(
        {
            "pod.name": "attr.k8s.pod.name",
            "service.name": "attr.k8s.service.name",
            "service.namespace": "attr.k8s.namespace.name",
        }
    )

    attributes = pl.Struct(
        [
            pl.Field("jvm.gc.action", pl.String),
            pl.Field("jvm.gc.name", pl.String),
        ]
    )
    lf = unnest_json_col(lf, "Attributes", attributes)
    lf = lf.rename(
        {
            "jvm.gc.action": "attr.jvm.gc.action",
            "jvm.gc.name": "attr.jvm.gc.name",
        }
    )

    lf = lf.sort("time")
    return lf


def convert_traces(src: Path) -> pl.LazyFrame:
    lf = pl.scan_parquet(src).select(
        "Timestamp",
        "TraceId",
        "SpanId",
        "ParentSpanId",
        "SpanName",
        "SpanKind",
        "ServiceName",
        "ResourceAttributes",
        "SpanAttributes",
        "Duration",
        "StatusCode",
    )

    lf = replace_time_col(lf, "Timestamp")

    lf = lf.rename(
        {
            "TraceId": "trace_id",
            "SpanId": "span_id",
            "ParentSpanId": "parent_span_id",
            "SpanName": "span_name",
            "ServiceName": "service_name",
            "Duration": "duration",
            "SpanKind": "attr.span_kind",
            "StatusCode": "attr.status_code",
        }
    )

    resource_attributes = pl.Struct(
        [
            pl.Field("pod.name", pl.String),
            pl.Field("service.name", pl.String),
            pl.Field("service.namespace", pl.String),
        ]
    )
    lf = unnest_json_col(lf, "ResourceAttributes", resource_attributes)
    lf = lf.rename(
        {
            "pod.name": "attr.k8s.pod.name",
            "service.name": "attr.k8s.service.name",
            "service.namespace": "attr.k8s.namespace.name",
        }
    )

    span_attributes = pl.Struct(
        [
            # "telemetry.sdk.language": "go"
            pl.Field("http.method", pl.String),
            pl.Field("http.request_content_length", pl.String),
            pl.Field("http.response_content_length", pl.String),
            pl.Field("http.status_code", pl.String),
            #
            # "telemetry.sdk.language": "java"
            pl.Field("http.request.method", pl.String),
            pl.Field("http.response.status_code", pl.String),
            #
        ]
    )
    lf = unnest_json_col(lf, "SpanAttributes", span_attributes)

    coalesce_columns = [
        ("http.request.method", "http.method"),
        ("http.response.status_code", "http.status_code"),
    ]
    lf = lf.with_columns([pl.coalesce(*cols).alias(cols[0]) for cols in coalesce_columns])
    lf = lf.drop([cols[1] for cols in coalesce_columns])

    lf = lf.with_columns(
        pl.col("http.request_content_length").cast(pl.UInt64),
        pl.col("http.response_content_length").cast(pl.UInt64),
        pl.col("http.response.status_code").cast(pl.UInt16),
    )

    lf = lf.rename(
        {
            "http.request.method": "attr.http.request.method",
            "http.response.status_code": "attr.http.response.status_code",
            "http.request_content_length": "attr.http.request.content_length",
            "http.response_content_length": "attr.http.response.content_length",
        }
    )

    lf = lf.sort("time")

    return lf


def convert_logs(src: Path) -> pl.LazyFrame:
    lf = pl.scan_parquet(src).select(
        "Timestamp",
        "TraceId",
        "SpanId",
        "SeverityText",
        "ServiceName",
        "Body",
        "ResourceAttributes",
        # "LogAttributes",
    )

    lf = replace_time_col(lf, "Timestamp")

    lf = lf.rename(
        {
            "TraceId": "trace_id",
            "SpanId": "span_id",
            "ServiceName": "service_name",
            "SeverityText": "level",
            "Body": "message",
        }
    )

    lf = lf.with_columns(
        pl.col("level").str.to_uppercase(),
    )

    resource_attributes = pl.Struct(
        [
            pl.Field("pod.name", pl.String),
            pl.Field("service.name", pl.String),
            pl.Field("service.namespace", pl.String),
        ]
    )
    lf = unnest_json_col(lf, "ResourceAttributes", resource_attributes)
    lf = lf.rename(
        {
            "pod.name": "attr.k8s.pod.name",
            "service.name": "attr.k8s.service.name",
            "service.namespace": "attr.k8s.namespace.name",
        }
    )

    lf = lf.sort("time")

    return lf


class RcabenchDatapackLoader(DatapackLoader):
    def __init__(self, src_folder: Path, datapack: str) -> None:
        self._src_folder = src_folder
        self._datapack = datapack

        self._service = rcabench_get_service_name(datapack)

        injection = load_json(path=self._src_folder / "injection.json")
        self._fault_type: str = FAULT_TYPES[injection["fault_type"]]
        self._injection_config = json.loads(injection["display_config"])

    def name(self) -> str:
        return self._datapack

    def labels(self) -> list[Label]:
        if self._fault_type.startswith("Network"):
            injection_point = self._injection_config.get("injection_point")
            assert isinstance(injection_point, dict)

            source_service = injection_point.get("source_service")
            target_service = injection_point.get("target_service")

            if source_service and target_service:
                return [
                    Label(level="service", name=source_service),
                    Label(level="service", name=target_service),
                ]

        return [Label(level="service", name=self._service)]

    def data(self) -> dict[str, Any]:
        ans: dict[str, Any] = {
            "env.json": self._src_folder / "env.json",
            "injection.json": self._src_folder / "injection.json",
        }

        converters = {
            "_traces": convert_traces,
            "_logs": convert_logs,
            "_metrics": convert_metrics,
            "_metrics_histogram": convert_metrics_histogram,
        }

        for key, func in converters.items():
            for prefix in ("normal", "abnormal"):
                name = f"{prefix}{key}.parquet"
                ans[name] = func(self._src_folder / name)

        return ans

    def postprocess(self, folder: Path, files: set[str]) -> None:
        schema = postprocess_collect_schema(folder, files)
        save_json(schema, path=folder / "schema.json")


@timeit()
def scan_datapacks(src_root: Path) -> list[str]:
    datapacks = []
    for path in src_root.iterdir():
        if not path.is_dir():
            continue

        if not (path / "injection.json").exists():
            continue

        total_size = 0
        for file in path.iterdir():
            assert file.is_file()
            total_size += file.stat().st_size
        total_size_mib = total_size / (1024 * 1024)

        if total_size_mib > 500:
            logger.warning(f"Skipping large datapack `{path.name}` with size {total_size_mib:.2f} MiB")
            continue

        mtime = path.stat().st_mtime
        datapacks.append((path.name, mtime))

    datapacks.sort(key=lambda x: x[1])

    return [name for name, _ in datapacks]


class RcabenchDatasetLoader(DatasetLoader):
    def __init__(self, src_root: Path, dataset: str) -> None:
        self._src_root = src_root
        self._dataset = dataset

        self._datapacks = scan_datapacks(src_root)

    def name(self) -> str:
        return self._dataset

    def __len__(self) -> int:
        return len(self._datapacks)

    def __getitem__(self, index: int) -> DatapackLoader:
        datapack = self._datapacks[index]
        return RcabenchDatapackLoader(
            src_folder=self._src_root / datapack,
            datapack=datapack,
        )


@app.command()
@timeit()
def run(skip: bool = True, parallel: int | None = 4):
    src_root = Path("data") / "rcabench_dataset"

    loader = RcabenchDatasetLoader(src_root, dataset="rcabench")

    convert_dataset(
        loader,
        skip=skip,
        parallel=parallel,
        ignore_exceptions=True,
    )


@app.command()
@timeit()
def local_test_1():
    datapack = "ts0-ts-preserve-service-response-delay-jqrgnc"
    loader = RcabenchDatapackLoader(
        src_folder=Path("data") / "rcabench_dataset" / datapack,
        datapack=datapack,
    )
    convert_datapack(
        loader,
        dst_folder=Path("temp") / "rcabench" / datapack,
        skip=False,
    )


def _convert_time(ts: int, tz: datetime.tzinfo | None) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts, tz=datetime.UTC).replace(tzinfo=tz).astimezone(datetime.UTC)


def _task_scan_datapack_attributes(dataset: str, datapack: str, input_folder: Path) -> dict[str, Any]:
    attrs: dict[str, Any] = {"dataset": dataset, "datapack": datapack}

    env = load_json(path=input_folder / "env.json")
    injection = load_json(path=input_folder / "injection.json")

    tz = dateutil.tz.gettz(env["TIMEZONE"])
    normal_start = _convert_time(int(env["NORMAL_START"]), tz)
    normal_end = _convert_time(int(env["NORMAL_END"]), tz)
    abnormal_start = _convert_time(int(env["ABNORMAL_START"]), tz)
    abnormal_end = _convert_time(int(env["ABNORMAL_END"]), tz)

    attrs["inject_time"] = abnormal_start

    attrs["injection.fault_type"] = FAULT_TYPES[injection["fault_type"]]
    attrs["injection.display_config"] = injection["display_config"]
    attrs["injection.engine_config"] = injection["engine_config"]

    attrs["env.normal_start"] = normal_start
    attrs["env.normal_end"] = normal_end
    attrs["env.abnormal_start"] = abnormal_start
    attrs["env.abnormal_end"] = abnormal_end

    return attrs


@app.command()
def scan_datapack_attributes():
    dataset = "rcabench"
    datapacks = get_datapack_list(dataset)

    tasks = []
    for datapack, input_folder in datapacks:
        tasks.append(functools.partial(_task_scan_datapack_attributes, dataset, datapack, input_folder))

    results = fmap_threadpool(tasks, parallel=32)

    df = pl.DataFrame(results).sort("inject_time", descending=True)

    save_parquet(df, path=META_ROOT / dataset / "attributes.parquet")


if __name__ == "__main__":
    app()
