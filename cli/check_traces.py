#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.clients.clickhouse import get_clickhouse_client, query_parquet_stream
from rcabench_platform.v2.config import get_config
from rcabench_platform.v2.utils.fmap import fmap_threadpool
from rcabench_platform.v2.utils.serde import save_parquet

from tempfile import TemporaryDirectory
from pathlib import Path
import functools

import polars as pl


@app.command()
@timeit()
def check_loadgenerator(limit: int = 1000, parallel: int = 16):
    assert limit > 0

    temp = get_config().temp

    logger.info("Checking loadgenerator traces")

    with get_clickhouse_client() as client:
        query = f"""
        SELECT      *
        FROM        otel_traces
        WHERE       ServiceName = 'loadgenerator-service' AND ParentSpanId = ''
        ORDER BY    Timestamp DESC
        LIMIT       {limit}
        """
        save_path = temp / "loadgenerator_traces.parquet"
        query_parquet_stream(client, query, save_path)

    df: pl.DataFrame = pl.read_parquet(save_path)
    assert len(df) > 0, "No traces found"

    trace_id_list = df["TraceId"].unique().to_list()
    logger.info(f"Found {len(trace_id_list)} unique traces")

    with TemporaryDirectory() as tempdir:
        tasks = [functools.partial(_query_trace, trace_id, Path(tempdir)) for trace_id in trace_id_list]
        df_paths = fmap_threadpool(tasks, parallel=parallel)
        df = pl.read_parquet(df_paths)

    save_parquet(df, path=temp / "loadgenerator_traces_all.parquet")

    tasks = [functools.partial(_check_trace, trace_id, df) for trace_id in trace_id_list]
    results = fmap_threadpool(tasks, parallel=parallel)
    invalid_count = sum(not result for result in results)

    if invalid_count > 0:
        logger.error(f"Found {invalid_count}/{len(trace_id_list)} traces with no spans from other services")
    else:
        logger.info(f"All {len(trace_id_list)} traces have spans from other services")


@timeit(log_args={"trace_id"})
def _query_trace(trace_id: str, save_dir: Path) -> Path:
    with get_clickhouse_client() as client:
        query = f"""
        SELECT      *
        FROM        otel_traces
        WHERE       TraceId = '{trace_id}'
        ORDER BY    Timestamp ASC
        LIMIT       1000
        """
        temp_path = save_dir / f"{trace_id}.parquet"
        query_parquet_stream(client, query, temp_path)
        return temp_path


def _check_trace(trace_id: str, df: pl.DataFrame) -> bool:
    trace_df = df.filter(
        pl.col("TraceId") == trace_id,
        pl.col("ServiceName") != "loadgenerator-service",
    )
    if len(trace_df) == 0:
        logger.warning(f"Trace `{trace_id}` has no spans from other services")
        return False
    return True


if __name__ == "__main__":
    app()
