#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit

from rcabench_platform.v2.clients.clickhouse import get_clickhouse_client, query_parquet_stream
from rcabench_platform.v2.clients.k8s import download_kube_info
from rcabench_platform.v2.clients.rcabench_ import get_rcabench_openapi_client

from rcabench_platform.v2.utils.fmap import fmap_processpool, fmap_threadpool
from rcabench_platform.v2.utils.serde import save_json

from pathlib import Path
from typing import Any
import subprocess
import traceback
import functools
import tempfile
import shutil
import os

import pandas as pd


@app.command()
@timeit()
def ping_clickhouse() -> None:
    with get_clickhouse_client() as client:
        assert client.ping(), "clickhouse should be reachable"
        logger.info("clickhouse is reachable")


def convert_to_clickhouse_time(unix_timestamp: int, tz: str) -> str:
    """将 UNIX 时间戳转换为 ClickHouse 支持的时间格式"""
    return (
        pd.to_datetime(unix_timestamp, utc=True, unit="s")
        .astimezone(tz)  # type:ignore
        .strftime("%Y-%m-%d %H:%M:%S")
    )


@timeit()
def query_metrics(save_path: Path, namespace: str, start_time: str, end_time: str):
    query = f"""
    SELECT
        TimeUnix,
        MetricName,
        MetricDescription,
        Value,
        multiIf(
                om.Attributes['source_workload'] != '', 
                om.Attributes['source_workload'],
                om.Attributes['destination_workload'] != '', 
                om.Attributes['destination_workload'],
                om.ResourceAttributes['k8s.deployment.name'] != '', 
                om.ResourceAttributes['k8s.deployment.name'],
                om.ResourceAttributes['k8s.statefulset.name'] != '', 
                om.ResourceAttributes['k8s.statefulset.name'],
                om.ServiceName
        ) AS ServiceName,
        MetricUnit,
        toJSONString(ResourceAttributes) AS ResourceAttributes,
        toJSONString(Attributes) AS Attributes
    FROM
        otel_metrics_gauge om
    WHERE
        (
            om.ResourceAttributes['k8s.namespace.name'] = '{namespace}'
            OR om.Attributes['destination_namespace'] = '{namespace}'
            OR om.Attributes['source_namespace'] = '{namespace}'
        )
        AND om.TimeUnix BETWEEN '{start_time}' AND '{end_time}'
    """

    with get_clickhouse_client() as client:
        query_parquet_stream(client, query, save_path)


@timeit()
def query_metrics_sum(save_path: Path, namespace: str, start_time: str, end_time: str):
    query = f"""
    SELECT
        TimeUnix,
        MetricName,
        MetricDescription,
        Value,
        multiIf(
                omg.Attributes['source_workload'] != '', omg.Attributes['source_workload'],
                omg.Attributes['destination_workload'] != '', omg.Attributes['destination_workload'],
                omg.ResourceAttributes['k8s.deployment.name'] != '', omg
                .ResourceAttributes['k8s.deployment.name'],
                omg.ResourceAttributes['k8s.statefulset.name'] != '', omg
                .ResourceAttributes['k8s.statefulset.name'],
                omg.ServiceName
        ) AS ServiceName,
        MetricUnit,
        toJSONString(ResourceAttributes) AS ResourceAttributes,
        toJSONString(Attributes) AS Attributes
    FROM
        otel_metrics_sum omg
    WHERE
        (
            omg.ResourceAttributes['k8s.namespace.name'] = '{namespace}' 
            OR omg.ResourceAttributes['service.namespace'] = '{namespace}'
            OR omg.Attributes['destination_namespace'] = '{namespace}'
            OR omg.Attributes['source_namespace'] = '{namespace}'
            OR (omg.Attributes['destination'] LIKE '{namespace}/%')
            OR (omg.Attributes['source'] LIKE '{namespace}/%')
        )
        AND omg.TimeUnix BETWEEN '{start_time}' AND '{end_time}'
    """

    with get_clickhouse_client() as client:
        query_parquet_stream(client, query, save_path)


@timeit()
def query_metrics_histogram(save_path: Path, namespace: str, start_time: str, end_time: str):
    query = f"""
    SELECT
        TimeUnix,
        MetricName,
        multiIf(
            omh.Attributes['source_workload'] != '', omh.Attributes['source_workload'],
            omh.Attributes['destination_workload'] != '', omh.Attributes['destination_workload'],
            omh.ResourceAttributes['k8s.deployment.name'] != '', omh
            .ResourceAttributes['k8s.deployment.name'],
            omh.ResourceAttributes['k8s.statefulset.name'] != '', omh
            .ResourceAttributes['k8s.statefulset.name'],
            omh.ServiceName
        ) AS ServiceName,
        MetricUnit,
        toJSONString(ResourceAttributes) AS ResourceAttributes,
        toJSONString(Attributes) AS Attributes,
        Count,
        Sum,
        BucketCounts,
        ExplicitBounds,
        Min,
        Max,
        AggregationTemporality
    FROM
        otel_metrics_histogram omh
    WHERE
        (
            omh.ResourceAttributes['k8s.namespace.name'] = '{namespace}' 
            OR omh.ResourceAttributes['service.namespace'] = '{namespace}'
            OR omh.Attributes['destination_namespace'] = '{namespace}'
            OR omh.Attributes['source_namespace'] = '{namespace}'
            OR (omh.Attributes['destination'] LIKE '{namespace}/%')
            OR (omh.Attributes['source'] LIKE '{namespace}/%')
        )
        AND omh.TimeUnix BETWEEN '{start_time}' AND '{end_time}'
    """

    with get_clickhouse_client() as client:
        query_parquet_stream(client, query, save_path)


@timeit()
def query_logs(save_path: Path, namespace: str, start_time: str, end_time: str):
    query = f"""
    SELECT
        Timestamp,
        TimestampTime,
        TraceId,
        SpanId,
        SeverityText,
        SeverityNumber,
        ServiceName,
        Body,
        toJSONString(ResourceAttributes) AS ResourceAttributes,
        toJSONString(LogAttributes) as LogAttributes
    FROM
        otel_logs ol
    WHERE
        ol.ResourceAttributes['service.namespace'] = '{namespace}'
        AND ol.Timestamp BETWEEN '{start_time}' AND '{end_time}'
    """

    with get_clickhouse_client() as client:
        query_parquet_stream(client, query, save_path)


@timeit()
def query_traces(save_path: Path, namespace: str, start_time: str, end_time: str):
    query = f"""
    SELECT 
        Timestamp,
        TraceId,
        SpanId,
        ParentSpanId,
        TraceState,
        SpanName,
        SpanKind,
        ServiceName,
        toJSONString(ResourceAttributes) AS ResourceAttributes,
        toJSONString(SpanAttributes) AS SpanAttributes,
        Duration,
        StatusCode,
        StatusMessage
    FROM
        otel_traces ot
    WHERE
        ot.ResourceAttributes['service.namespace'] = '{namespace}'
        AND ot.Timestamp BETWEEN '{start_time}' AND '{end_time}'
    """

    with get_clickhouse_client() as client:
        query_parquet_stream(client, query, save_path)


@timeit()
def query_trace_id_ts(save_path: Path, namespace: str, start_time: str, end_time: str):
    query = f"""
    SELECT 
        TraceId,
        Start,
        End
    FROM
        otel_traces_trace_id_ts
    WHERE
        Start BETWEEN '{start_time}' AND '{end_time}'
        AND End BETWEEN '{start_time}' AND '{end_time}'
    """

    with get_clickhouse_client() as client:
        query_parquet_stream(client, query, save_path)


@timeit()
def query_injection(rcabench_url: str, name: str):
    from rcabench.openapi import InjectionApi

    try:
        api = InjectionApi(get_rcabench_openapi_client(base_url=rcabench_url))
        resp = api.api_v1_injections_query_get(name=name)
        assert resp.data is not None
        return resp.data
    except Exception:
        traceback.print_exc()
        logger.error(f"Failed to query injection details: {name}")
        return None


@timeit()
def query_kube_info(namespace: str) -> dict[str, Any] | None:
    try:
        resp = download_kube_info(ns=namespace)
    except Exception:
        traceback.print_exc()
        logger.error(f"Failed to query kube info: {namespace}")
        return None

    return resp.to_dict()


@app.command()
@timeit()
def run(rcabench_url: str = "http://10.10.10.220:32080"):
    ping_clickhouse()

    # Prepare the output directory
    output_path = Path(os.environ["OUTPUT_PATH"])
    logger.debug(f"output_path: `{output_path}`")

    output_path.mkdir(parents=True, exist_ok=True)

    # Check input parameters
    namespace = os.environ["NAMESPACE"]
    assert namespace, "NAMESPACE must be set"

    timezone = os.environ["TIMEZONE"]
    assert timezone, "TIMEZONE must be set"

    normal_start = int(os.environ["NORMAL_START"])
    normal_end = int(os.environ["NORMAL_END"])

    abnormal_start = int(os.environ["ABNORMAL_START"])
    abnormal_end = int(os.environ["ABNORMAL_END"])

    normal_time_range = [normal_start, normal_end]
    abnormal_time_range = [abnormal_start, abnormal_end]

    logger.debug(f"normal_time_range:   `{normal_time_range}`")
    logger.debug(f"abnormal_time_range: `{abnormal_time_range}`")

    for time in [normal_start, normal_end, abnormal_start, abnormal_end]:
        assert time > 0 and len(str(time)) == 10, "unix timestamp in seconds"

    assert normal_start < normal_end <= abnormal_start < abnormal_end

    # support discontinuous time ranges?
    assert normal_end == abnormal_start, "The time ranges must be continuous for now"

    ch_normal_start = convert_to_clickhouse_time(normal_start, timezone)
    ch_normal_end = convert_to_clickhouse_time(normal_end, timezone)
    ch_abnormal_start = convert_to_clickhouse_time(abnormal_start, timezone)
    ch_abnormal_end = convert_to_clickhouse_time(abnormal_end, timezone)

    ch_normal_time_range = [ch_normal_start, ch_normal_end]
    ch_abnormal_time_range = [ch_abnormal_start, ch_abnormal_end]

    logger.debug(f"ch_normal_time_range:   `{ch_normal_time_range}`")
    logger.debug(f"ch_abnormal_time_range: `{ch_abnormal_time_range}`")

    # Download the data
    prefixes = ["normal", "abnormal"]
    time_ranges = [ch_normal_time_range, ch_abnormal_time_range]
    queries = {
        "metrics": query_metrics,
        "metrics_sum": query_metrics_sum,
        "metrics_histogram": query_metrics_histogram,
        "logs": query_logs,
        "traces": query_traces,
        "trace_id_ts": query_trace_id_ts,
    }

    with tempfile.TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)

        tasks = []
        for prefix, time_range in zip(prefixes, time_ranges):
            for query_name, query_func in queries.items():
                save_path = tempdir / f"{prefix}_{query_name}.parquet"
                tasks.append(functools.partial(query_func, save_path, namespace, time_range[0], time_range[1]))

        fmap_processpool(tasks, parallel=8)

        env_params = {
            "NAMESPACE": namespace,
            "TIMEZONE": timezone,
            "NORMAL_START": str(normal_start),
            "NORMAL_END": str(normal_end),
            "ABNORMAL_START": str(abnormal_start),
            "ABNORMAL_END": str(abnormal_end),
        }
        save_json(env_params, path=tempdir / "env.json")

        injection = query_injection(rcabench_url, output_path.name)
        if injection:
            save_json(injection.model_dump(), path=tempdir / "injection.json")

        kube_info = query_kube_info(namespace)
        if kube_info:
            save_json(kube_info, path=tempdir / "k8s.json")

        copy_files(tempdir, output_path)


@timeit()
def copy_files(src: Path, dst: Path):
    assert src.is_dir()
    assert dst.is_dir()

    subprocess.run("sha256sum * > sha256sum.txt", cwd=src, shell=True, check=True)

    tasks = []
    for file in src.iterdir():
        if file.is_file():
            tasks.append(functools.partial(shutil.copyfile, file, dst / file.name))

    fmap_threadpool(tasks, parallel=8)


@app.command()
def local_test():
    env_params = {
        "OUTPUT_PATH": "temp/ts5-ts-consign-service-partition-tstlvq",
        "NAMESPACE": "ts5",
        "TIMEZONE": "Asia/Shanghai",
        "NORMAL_START": "1752156428",
        "NORMAL_END": "1752156668",
        "ABNORMAL_START": "1752156668",
        "ABNORMAL_END": "1752156908",
    }

    for key, value in env_params.items():
        os.environ[key] = value

    Path(env_params["OUTPUT_PATH"]).mkdir(parents=True, exist_ok=True)

    run()


@app.command()
def patch_injection(rcabench_url: str = "http://10.10.10.220:32080"):
    from rcabench.openapi import InjectionApi

    api = InjectionApi(get_rcabench_openapi_client(base_url=rcabench_url))
    resp = api.api_v1_injections_get()
    assert resp.data is not None, "No cases found in the response"

    case_names = list(set([item.injection_name for item in resp.data if item.injection_name]))
    for dataset_name in case_names:
        injection = query_injection(rcabench_url, dataset_name)
        if injection:
            dataset_path = Path("/mnt/jfs/rcabench_dataset") / dataset_name
            save_json(injection.model_dump(), path=dataset_path / "injection.json")
            save_json(injection.model_dump(), path=dataset_path / "converted" / "injection.json")

            platform_path = Path("/mnt/jfs/rcabench-platform-v2/data/rcabench_with_issues") / dataset_name
            if platform_path.exists():
                json_path = platform_path / "injection.json"
                save_json(injection.model_dump(), path=json_path)
                os.chown(json_path, 1000, 1000)
        else:
            logger.warning(f"No injection details found for dataset: {dataset_name}")


if __name__ == "__main__":
    app()
