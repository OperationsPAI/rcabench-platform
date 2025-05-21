#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger

from rcabench_platform.v1.clients.clickhouse_ import ClickHouseClient, get_clickhouse_client
from rcabench_platform.v1.clients.k8s import download_kube_info
from rcabench_platform.v1.clients.rcabench_ import CustomRCABenchSDK

from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.utils.fmap import fmap_processpool, fmap_threadpool
from rcabench_platform.v1.utils.serde import save_json

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


def query_parquet_stream(client: ClickHouseClient, query: str, save_path: Path):
    assert save_path.suffix == ".parquet", "save_path must be a parquet file"
    assert save_path.parent.is_dir(), "save_path parent must be a directory"

    stream = client.raw_stream(query=query, fmt="Parquet")
    with open(save_path, "wb") as f:
        for chunk in stream:
            f.write(chunk)
        f.flush()


@timeit()
def query_metrics(save_path: Path, namespace: str, start_time: str, end_time: str):
    query = f"""
    SELECT
        TimeUnix,
        MetricName,
        MetricDescription,
        Value,
        ServiceName,
        MetricUnit,
        toJSONString(ResourceAttributes) AS ResourceAttributes,
        toJSONString(Attributes) AS Attributes
    FROM
        otel_metrics_gauge om
    WHERE
        om.ResourceAttributes['k8s.namespace.name'] = '{namespace}'
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
        ServiceName,
        MetricUnit,
        toJSONString(ResourceAttributes) AS ResourceAttributes,
        toJSONString(Attributes) AS Attributes
    FROM
        otel_metrics_sum omg
    WHERE
        (omg.ResourceAttributes['k8s.namespace.name'] = '{namespace}' OR omg.ResourceAttributes['service.namespace'] = '{namespace}')
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
        ServiceName,
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
        omh.ResourceAttributes['service.namespace'] = '{namespace}'
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
def query_injection(name: str) -> dict[str, Any] | None:
    sdk = CustomRCABenchSDK()

    try:
        resp = sdk.query_injection(name=name)
    except Exception:
        traceback.print_exc()
        logger.error(f"Failed to query injection: {name}")
        return None

    return resp


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
def run():
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

    # TODO: discontinuous time ranges?
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

        injection = query_injection(output_path.name)
        if injection:
            save_json(injection, path=tempdir / "injection.json")

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
        "OUTPUT_PATH": "/mnt/jfs/temp/ts5-ts-order-service-return-bbzg49",
        "NAMESPACE": "ts5",
        "TIMEZONE": "Asia/Shanghai",
        "NORMAL_START": "1747815230",
        "NORMAL_END": "1747815470",
        "ABNORMAL_START": "1747815470",
        "ABNORMAL_END": "1747815708",
    }

    for key, value in env_params.items():
        os.environ[key] = value

    Path(env_params["OUTPUT_PATH"]).mkdir(parents=True, exist_ok=True)

    run()


if __name__ == "__main__":
    app()
