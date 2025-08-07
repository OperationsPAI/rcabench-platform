#!/usr/bin/env -S uv run -s
# Configure matplotlib backend early for multi-processing safety
import matplotlib
import polars as pl

from rcabench_platform.v2.datasets.rcabench import valid
from rcabench_platform.v2.datasets.spec import get_dataset_meta_file

matplotlib.use("Agg")
import functools
import json
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.figure
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure
from rcabench.openapi import DtoDatapackDetectorReq, DtoDetectorRecord, EvaluationApi

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.utils.fmap import fmap_processpool, fmap_threadpool

# ================== Constants and Configuration ==================

RULE_COLUMNS = [
    "rule_1_network_no_direct_calls",
    "rule_2_http_method_same",
    "rule_3_http_no_direct_calls",
    "rule_4_single_point_no_calls",
    "rule_5_duplicated_spans",
    "rule_6_large_latency_normal",
    "rule_7_absolute_abnormal",
]

CATEGORY_CONFIG = {
    "categories": [
        "both_latency_and_success_rate",
        "latency_only",
        "success_rate_only",
        "no_issues",
        "absolute_anomaly",
    ],
    "display_labels": [
        "Both Latency & Success Rate Issues",
        "Latency Issues Only",
        "Success Rate Issues Only",
        "No Issues",
        "Absolute Anomaly",
    ],
    "colors": ["#ff6b6b", "#feca57", "#48cae4", "#a8e6cf", "#9d4edd"],
}

# Plot configuration
PLOT_CONFIG: dict[str, Any] = {
    "figure_size": (15, 4),
    "dpi": 300,
    "interval_minutes": 1,
    "marker_size": 1,
    "line_width": 0.8,
    "alpha": 0.7,
}

# ================== File and Data Utilities ==================


def read_dataframe(file: Path) -> pl.LazyFrame:
    """Read parquet file as a Polars LazyFrame."""
    return pl.scan_parquet(file)


def extract_status_code(span_attributes: str) -> str:
    """Extract HTTP status code from span attributes."""
    try:
        ra = json.loads(span_attributes) if span_attributes else {}
        return ra["http.status_code"]
    except Exception:
        return "-1"


# ================== Data Preparation Functions ==================


def prepare_trace_data(datapack: Path) -> tuple[pl.DataFrame, pl.DataFrame, Any, Any]:
    """Load and prepare trace data for visualization."""
    df1: pl.DataFrame = pl.scan_parquet(datapack / "normal_traces.parquet").collect()
    df2: pl.DataFrame = pl.scan_parquet(datapack / "abnormal_traces.parquet").collect()
    start_time = df1.select(pl.col("Timestamp").min()).item()
    last_normal_time = df1.select(pl.col("Timestamp").max()).item()

    df1 = df1.with_columns(pl.lit("normal").alias("trace_type"))
    df2 = df2.with_columns(pl.lit("abnormal").alias("trace_type"))

    return df1, df2, start_time, last_normal_time


def prepare_entry_data(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """Prepare entry point data from trace data."""
    merged_df: pl.DataFrame = pl.concat([df1, df2])
    entry_df: pl.DataFrame = merged_df.filter(
        (pl.col("ServiceName") == "loadgenerator") & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
    )

    if len(entry_df) == 0:
        logger.error("loadgenerator not found in trace data, using ts-ui-dashboard as fallback")
        entry_df = merged_df.filter(
            (pl.col("ServiceName") == "ts-ui-dashboard")
            & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
        )

    if len(entry_df) == 0:
        logger.error("No valid entrypoint found in trace data")
        return pl.DataFrame()

    entry_df = entry_df.with_columns(
        [
            pl.col("Timestamp").alias("datetime"),
            (pl.col("Duration") / 1e9).alias("duration"),
            pl.struct(["SpanAttributes", "StatusCode"])
            .map_elements(lambda x: extract_status_code(x["SpanAttributes"]), return_dtype=pl.Utf8)
            .alias("status_code"),
        ]
    ).sort("Timestamp")

    entry_df = entry_df.with_columns(
        pl.col("SpanName").map_elements(extract_path, return_dtype=pl.Utf8).alias("api_path")
    )

    return entry_df


# ================== Main Visualization Functions ==================


def create_span_visualization(
    entry_df: pl.DataFrame,
    issue_data: list[DtoDetectorRecord],
    datapack_name: str,
    output_file: Path,
    start_time: Any,
    last_normal_time: Any,
) -> None:
    problematic_spans = set()
    for record in issue_data:
        problematic_spans.add(record.span_name)

    if not problematic_spans:
        logger.info(f"No specific problematic spans found in {datapack_name}")
        return

    # Create figure with subplots - 2 columns for each span (latency and status code)
    fig, axes = plt.subplots(len(problematic_spans), 2, figsize=(20, 6 * len(problematic_spans)), dpi=300)
    if len(problematic_spans) == 1:
        axes = axes.reshape(1, -1)

    # Plot each problematic span
    for idx, span_name in enumerate(problematic_spans):
        ax_latency = axes[idx, 0]
        ax_status = axes[idx, 1]

        span_data = entry_df.filter(pl.col("api_path") == span_name)

        if len(span_data) == 0:
            for ax in [ax_latency, ax_status]:
                ax.text(
                    0.5,
                    0.5,
                    f"No data found for {span_name}",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                )
            ax_latency.set_title(f"{span_name} - Latency")
            ax_status.set_title(f"{span_name} - Status Code")
            continue

        # Separate normal and abnormal data
        normal_data = span_data.filter(pl.col("trace_type") == "normal")
        abnormal_data = span_data.filter(pl.col("trace_type") == "abnormal")

        normal_count = len(normal_data)
        abnormal_count = len(abnormal_data)

        # Plot latency over time
        if len(normal_data) > 0:
            # Sort normal data by datetime for proper line connection
            normal_sorted = normal_data.sort("datetime")
            normal_times = normal_sorted.select("datetime").to_numpy().flatten()
            normal_latencies = normal_sorted.select("duration").to_numpy().flatten()
            ax_latency.plot(
                normal_times,
                normal_latencies,
                color="green",
                alpha=0.7,
                linewidth=1.2,
                marker="o",
                markersize=3,
                label=f"Normal ({normal_count})",
            )

        if len(abnormal_data) > 0:
            # Sort abnormal data by datetime for proper line connection
            abnormal_sorted = abnormal_data.sort("datetime")
            abnormal_times = abnormal_sorted.select("datetime").to_numpy().flatten()
            abnormal_latencies = abnormal_sorted.select("duration").to_numpy().flatten()
            ax_latency.plot(
                abnormal_times,
                abnormal_latencies,
                color="red",
                alpha=0.7,
                linewidth=1.2,
                marker="o",
                markersize=3,
                label=f"Abnormal ({abnormal_count})",
            )

        # Add vertical line at last normal time
        ax_latency.axvline(x=last_normal_time, color="blue", linestyle="--", alpha=0.7, label="Last Normal Time")

        ax_latency.set_xlabel("Time")
        ax_latency.set_ylabel("Latency (seconds)")
        ax_latency.set_title(f"{span_name} - Latency\n(Normal: {normal_count}, Abnormal: {abnormal_count})")
        ax_latency.legend()
        ax_latency.grid(True, alpha=0.3)

        # Format x-axis for datetime
        ax_latency.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax_latency.tick_params(axis="x", rotation=45)

        # Plot status codes over time
        # Convert status codes to numeric for plotting
        if len(normal_data) > 0:
            # Use sorted data for consistent time ordering
            normal_sorted = normal_data.sort("datetime")
            normal_times = normal_sorted.select("datetime").to_numpy().flatten()
            normal_status = normal_sorted.select("status_code").to_numpy().flatten()
            normal_status_numeric = [int(s) if s.isdigit() else -1 for s in normal_status]
            ax_status.scatter(
                normal_times, normal_status_numeric, color="green", alpha=0.6, s=10, label=f"Normal ({normal_count})"
            )

        if len(abnormal_data) > 0:
            # Use sorted data for consistent time ordering
            abnormal_sorted = abnormal_data.sort("datetime")
            abnormal_times = abnormal_sorted.select("datetime").to_numpy().flatten()
            abnormal_status = abnormal_sorted.select("status_code").to_numpy().flatten()
            abnormal_status_numeric = [int(s) if s.isdigit() else -1 for s in abnormal_status]
            ax_status.scatter(
                abnormal_times,
                abnormal_status_numeric,
                color="red",
                alpha=0.6,
                s=10,
                label=f"Abnormal ({abnormal_count})",
            )

        # Add vertical line at last normal time
        ax_status.axvline(x=last_normal_time, color="blue", linestyle="--", alpha=0.7, label="Last Normal Time")

        ax_status.set_xlabel("Time")
        ax_status.set_ylabel("HTTP Status Code")
        ax_status.set_title(f"{span_name} - Status Code\n(Normal: {normal_count}, Abnormal: {abnormal_count})")
        ax_status.legend()
        ax_status.grid(True, alpha=0.3)

        # Format x-axis for datetime
        ax_status.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax_status.tick_params(axis="x", rotation=45)

        # Set y-axis to show common HTTP status codes
        status_codes = [200, 400, 401, 403, 404, 500, 502, 503, 504]
        ax_status.set_yticks(status_codes)

    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight")
    plt.close()
    logger.info(f"Visualization saved to {output_file}")


def vis_call(datapack: Path, skip_existing: bool = True, output_dir: Path | None = None) -> None:
    with RCABenchClient() as client:
        eval_api = EvaluationApi(client)
        resp = eval_api.api_v2_evaluations_datapacks_detector_post(
            request=DtoDatapackDetectorReq(
                datapacks=[datapack.name],
            )
        )
        assert resp.code and resp.code < 300
        assert resp.data is not None and resp.data.items is not None, "No detector results found"
        data: list[DtoDetectorRecord] = [i.results for i in resp.data.items if i.results is not None][0]

    if data is None or len(data) == 0:
        logger.warning(f"No detector results found for {datapack.name}, skipping visualization")
        return
    issue_data = [i for i in data if i.issue is not None and i.issue != "{}"]
    if len(issue_data) == 0:
        logger.info(f"No issues found in {datapack.name}, skipping visualization")
        return

    # Prepare trace data
    normal_df, abnormal_df, start_time, last_normal_time = prepare_trace_data(datapack)

    if normal_df.is_empty() or abnormal_df.is_empty() or start_time is None or last_normal_time is None:
        logger.error(f"Invalid trace data in {datapack.name}, skipping visualization")
        return

    if output_dir is not None:
        final_output_dir = output_dir
    else:
        hour_key: str = start_time.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d_%H")
        final_output_dir: Path = Path("temp") / "vis_by_hour" / hour_key

    final_output_dir.mkdir(parents=True, exist_ok=True)
    output_file: Path = final_output_dir / f"{datapack.name}.png"

    if output_file.exists() and skip_existing:
        return

    # Prepare entry data
    entry_df = prepare_entry_data(normal_df, abnormal_df)
    if len(entry_df) == 0:
        return

    # Create visualization for problematic spans
    create_span_visualization(entry_df, issue_data, datapack.name, output_file, start_time, last_normal_time)


@app.command(name="vis-single-entry")
@timeit()
def visualize_latency(datapack: str):
    datapack_path = Path("data") / "rcabench_dataset" / datapack
    if not datapack_path.exists():
        logger.error(f"Datapack not found: {datapack_path}")
        return
    vis_call(datapack_path, skip_existing=False)


@app.command(name="vis-batch")
def batch_visualize(skip_existing: bool = True) -> None:
    datapack_path = Path("data") / "rcabench_dataset"
    if not datapack_path.exists():
        logger.error(f"Datapack directory not found: {datapack_path}")
        return

    validation_tasks = [functools.partial(valid, datapack_path / p.name) for p in datapack_path.iterdir() if p.is_dir()]
    datapacks = fmap_processpool(validation_tasks, parallel=32, ignore_exceptions=True, cpu_limit_each=2)

    tasks = [
        functools.partial(vis_call, datapack=p[0], skip_existing=skip_existing, output_dir=None)
        for p in datapacks
        if p[1]
    ]

    fmap_processpool(tasks, parallel=32, cpu_limit_each=2)


if __name__ == "__main__":
    app()
