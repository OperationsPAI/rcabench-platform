#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.clients.clickhouse import get_clickhouse_client, query_parquet_stream
from rcabench_platform.v2.config import get_config
from rcabench_platform.v2.utils.dataframe import print_dataframe
from rcabench_platform.v2.utils.fmap import fmap_threadpool
from rcabench_platform.v2.utils.serde import save_parquet

import functools

import polars as pl


@app.command()
@timeit()
def check_loadgenerator(limit: int = 10000, parallel: int = 16):
    assert limit > 0

    temp = get_config().temp

    logger.info("Checking loadgenerator traces")

    with get_clickhouse_client() as client:
        query = f"""
        WITH target_traces AS (
            SELECT      TraceId
            FROM        otel_traces
            WHERE       ServiceName = 'loadgenerator-service' AND ParentSpanId = ''
            ORDER BY    Timestamp DESC
            LIMIT       {limit}
        )
        SELECT      *
        FROM        otel_traces
        WHERE       TraceId IN (SELECT TraceId FROM target_traces)
        ORDER BY    TraceId, Timestamp ASC
        """
        save_path = temp / "loadgenerator_traces_all.parquet"
        query_parquet_stream(client, query, save_path)

    df = pl.read_parquet(save_path)
    assert len(df) > 0, "No traces found"

    loadgenerator_df = df.filter(
        pl.col("ServiceName") == "loadgenerator-service",
        pl.col("ParentSpanId") == "",
    )
    trace_id_list = loadgenerator_df["TraceId"].unique().to_list()

    logger.info(f"Found {len(trace_id_list)} unique traces")

    tasks = [functools.partial(_check_trace, trace_id, df, loadgenerator_df) for trace_id in trace_id_list]
    results = fmap_threadpool(tasks, parallel=parallel)
    invalid_count = sum(not result for result in results)

    if invalid_count > 0:
        logger.error(f"Found {invalid_count}/{len(trace_id_list)} traces with no spans from other services")
    else:
        logger.info(f"All {len(trace_id_list)} traces have spans from other services")

    invalid = []
    for trace_id, result in zip(trace_id_list, results):
        invalid.append({"TraceId": trace_id, "check:invalid": not result})
    invalid_df = pl.DataFrame(invalid)
    df = df.join(invalid_df, on="TraceId", how="left")
    save_parquet(df, path=save_path)

    duration_df = df.select(pl.col("Duration").truediv(1e9).alias("duration")).select(
        pl.col("duration").mean().alias("duration:mean"),
        pl.col("duration").std().alias("duration:std"),
        pl.col("duration").min().alias("duration:min"),
        pl.col("duration").median().alias("duration:median"),
        pl.col("duration").max().alias("duration:max"),
        pl.col("duration").quantile(0.90).alias("duration:P90"),
        pl.col("duration").quantile(0.95).alias("duration:P95"),
        pl.col("duration").quantile(0.99).alias("duration:P99"),
        pl.col("duration").quantile(0.999).alias("duration:P999"),
        pl.col("duration").quantile(0.9999).alias("duration:P9999"),
    )
    print_dataframe(duration_df)


def _check_trace(trace_id: str, df: pl.DataFrame, loadgenerator_df: pl.DataFrame) -> bool:
    trace_df = df.filter(
        pl.col("TraceId") == trace_id,
        pl.col("ServiceName") != "loadgenerator-service",
    )
    if len(trace_df) == 0:
        timestamp = loadgenerator_df.filter(pl.col("TraceId") == trace_id)["Timestamp"].item()
        logger.warning(f"Trace `{trace_id}` ({timestamp}) has no spans from other services")
        return False
    return True


if __name__ == "__main__":
    app()
