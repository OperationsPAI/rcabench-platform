#!/usr/bin/env -S uv run -s
from collections import defaultdict
from dataclasses import dataclass
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.clients.clickhouse import get_clickhouse_client, query_parquet_stream
from rcabench_platform.v2.config import get_config
from rcabench_platform.v2.datasets.spec import get_datapack_folder, get_dataset_meta_file
from rcabench_platform.v2.utils.dataframe import print_dataframe
from rcabench_platform.v2.utils.fmap import fmap_threadpool
from rcabench_platform.v2.utils.serde import load_json, save_parquet

from fractions import Fraction
from datetime import datetime
from pathlib import Path
import functools

import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


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

    duration_lf = df.lazy().select(pl.col("Duration").truediv(1e9).alias("duration"))
    duration_df = duration_statistics(duration_lf).collect()

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


def duration_statistics(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.select(
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


@dataclass(kw_only=True, slots=True)
class DatapackCheck:
    datapack: str
    normal_start: datetime
    normal_end: datetime
    interference_start: datetime
    interference_end: datetime


@app.command()
@timeit()
def check_clickhouse():
    lf = pl.scan_parquet(get_dataset_meta_file("rcabench", "attributes.parquet"))

    lf = lf.filter(
        pl.col("inject_time") >= pl.datetime(2025, 7, 1, time_zone="UTC"),
        pl.col("inject_time") <= pl.datetime(2025, 7, 7, time_zone="UTC"),
    )

    lf = lf.select(
        pl.col("datapack"),
        pl.col("env.normal_start").alias("normal_start"),
        pl.col("env.normal_end").alias("normal_end"),
        pl.col("env.abnormal_end").alias("interference_start"),
        pl.col("env.abnormal_end").add(pl.duration(seconds=120)).alias("interference_end"),
    )

    df = lf.collect()

    checks = [DatapackCheck(**row) for row in df.iter_rows(named=True)]
    logger.info("checking {} datapacks", len(checks))

    candidates: defaultdict[str, list[tuple[datetime, datetime]]] = defaultdict(list)

    for check in checks:
        assert isinstance(check.normal_start, datetime)
        assert isinstance(check.interference_end, datetime)

        for other in checks:
            if check.datapack == other.datapack:
                continue
            if check.normal_end < other.interference_start:
                continue
            if check.normal_start > other.interference_end:
                continue
            candidates[check.datapack].append((other.interference_start, other.interference_end))

    logger.info("checking {} datapacks", len(candidates))

    tasks = [functools.partial(_check_clickhouse, datapack, ranges) for datapack, ranges in candidates.items()]
    results = fmap_threadpool(tasks, parallel=32)
    proportion = Fraction(sum(results), len(results))
    logger.info("interference proportion = {} ({:.2%})", proportion, float(proportion))


def _check_clickhouse(datapack: str, ranges: list[tuple[datetime, datetime]]):
    lf = pl.scan_parquet(get_datapack_folder("rcabench", datapack) / "normal_traces.parquet")
    lf = lf.select("time", (pl.col("duration") / 1e9).alias("duration"))

    condition = functools.reduce(
        lambda x, y: x & y,
        [(pl.col("time") < start) | (pl.col("time") > end) for start, end in ranges],
    )
    lhs_lf = lf.filter(condition)
    rhs_lf = lf.filter(condition.not_())

    lhs_df = duration_statistics(lhs_lf).collect()
    rhs_df = duration_statistics(rhs_lf).collect()

    lhs_duration = lhs_df["duration:P9999"].item()
    rhs_duration = rhs_df["duration:P9999"].item()

    if rhs_duration is None:
        return False

    if lhs_duration is None:
        return rhs_duration > 5

    assert isinstance(lhs_duration, float)
    assert isinstance(rhs_duration, float)

    logger.debug("datapack=`{}`, lhs={:.3}s, rhs={:.3}s", datapack, lhs_duration, rhs_duration)

    return (rhs_duration / lhs_duration) > 1.5


@app.command()
@timeit()
def concat_normal_ranges():
    lf = pl.scan_parquet(get_dataset_meta_file("rcabench", "attributes.parquet"))

    lf = lf.filter(
        pl.col("inject_time") >= pl.datetime(2025, 7, 7, 0, time_zone="UTC"),
        pl.col("inject_time") <= pl.datetime(2025, 7, 7, 12, time_zone="UTC"),
    )

    df = lf.select("datapack").collect()
    datapacks = df["datapack"].unique().to_list()

    logger.info("found {} datapacks", len(datapacks))

    lf_list: list[pl.LazyFrame] = []
    for datapack in datapacks:
        folder = get_datapack_folder("rcabench", datapack)
        file_path = folder / "normal_traces.parquet"
        if file_path.exists():
            lf = pl.scan_parquet(file_path)
            lf = lf.select("time", "duration")
            lf = lf.group_by_dynamic("time", every="1s").agg(pl.col("duration").max())
            lf_list.append(lf)

    df = pl.concat(lf_list).collect()

    # Convert duration from nanoseconds to seconds
    df = df.with_columns(pl.col("duration") / 1e9)

    # Sort by time for plotting
    df = df.sort("time")

    # Create the plot
    temp = get_config().temp
    plt.figure(figsize=(15, 8))

    # Convert to pandas for easier plotting with matplotlib
    df_pandas = df.to_pandas()
    del df
    # Plot duration over time
    plt.plot(df_pandas["time"], df_pandas["duration"], linewidth=0.8, alpha=0.7)

    # Format the plot
    plt.title("Maximum Duration Over Time (1-second aggregation)", fontsize=14, fontweight="bold")
    plt.xlabel("Time", fontsize=12)
    plt.ylabel("Duration (seconds)", fontsize=12)
    plt.yscale("log")  # Set y-axis to logarithmic scale
    plt.grid(True, alpha=0.3)

    # Format x-axis to show dates nicely
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.xticks(rotation=45)

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    # Save the plot
    output_path = temp / "normal_ranges_duration_timeline.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    logger.info(f"Saved duration timeline plot to {output_path}")

    plt.close()


if __name__ == "__main__":
    app()
