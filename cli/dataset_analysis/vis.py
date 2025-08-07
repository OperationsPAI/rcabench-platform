#!/usr/bin/env -S uv run -s
# Configure matplotlib backend early for multi-processing safety
import matplotlib
import polars as pl

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


def iter_datapacks(input_path: Path, file_name: str) -> Generator[tuple[Path, Path], None, None]:
    """Iterate through datapacks and yield valid datapack paths with the specified file."""
    for datapack in input_path.iterdir():
        if not datapack.is_dir():
            continue
        file = datapack / file_name
        if not file.exists():
            logger.warning(f"No {file_name} found for {datapack.name}, skipping")
            continue
        yield datapack, file


def read_json(file: Path) -> dict[str, Any]:
    """Read JSON file and return the data."""
    with open(file, encoding="utf-8") as f:
        return json.load(f)


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


# ================== Data Processing Functions ==================


def classify_issue_categories_multi(data: dict[str, Any]) -> list[str]:
    """Classify issues into multiple categories based on the data."""
    issue_cats = data.get("issue_categories", {})
    absolute_anomaly = data.get("absolute_anomaly", False)
    categories = []

    if issue_cats.get("both_latency_and_success_rate", 0) > 0:
        categories.append("both_latency_and_success_rate")
    if issue_cats.get("latency_only", 0) > 0:
        categories.append("latency_only")
    if issue_cats.get("success_rate_only", 0) > 0:
        categories.append("success_rate_only")

    if absolute_anomaly:
        categories.append("absolute_anomaly")
    if not categories:
        categories.append("no_issues")
    return categories


def collect_issues(
    input_path: Path,
) -> tuple[list[str], list[str], list[tuple[str, str]]]:
    """Collect datapack names with and without issues."""
    datapacks: list[str] = []
    no_issue_datapacks: list[str] = []
    errs: list[tuple[str, str]] = []

    for datapack, con in iter_datapacks(input_path, "conclusion.csv"):
        try:
            conclusion = pl.read_csv(con)
            non_empty_issues = conclusion.filter(
                (pl.col("Issues").is_not_null()) & (pl.col("Issues") != "") & (pl.col("Issues") != "{}")
            )
            if len(non_empty_issues) > 0:
                datapacks.append(datapack.name)
            else:
                no_issue_datapacks.append(datapack.name)
        except Exception as e:
            logger.error(f"Error processing {con}: {e}")
            errs.append((datapack.name, str(e)))
    return datapacks, no_issue_datapacks, errs


def process_datapack_confidence(datapack_path: Path, du: int) -> str | None:
    """Check if datapack meets duration confidence requirements."""
    if not datapack_path.is_dir():
        return None
    normal_traces_file = datapack_path / "normal_traces.parquet"
    if not normal_traces_file.exists():
        logger.warning(f"No normal_traces.parquet found for {datapack_path.name}, skipping")
        return None
    try:
        df = read_dataframe(normal_traces_file)
        max_duration = df.select(pl.col("Duration").max()).collect().item()
        if max_duration is not None and max_duration < du * 1e9:
            return datapack_path.name
    except Exception as e:
        logger.error(f"Error processing {datapack_path.name}: {e}")
    return None


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


def process_single_datapack_visualization(
    datapack_path: str,
    skip_existing: bool = True,
    output_dir: Path | None = None,
    base_path: str = "data/rcabench_dataset",
) -> tuple[str, bool, str]:
    """
    Process a single datapack for visualization in a thread-safe manner.

    Returns:
        tuple: (datapack_name, success, error_message)
    """
    try:
        matplotlib.use("Agg")  # Use non-interactive backend

        full_datapack_path = Path(base_path) / datapack_path

        if not full_datapack_path.exists():
            return datapack_path, False, f"Datapack not found: {full_datapack_path}"

        required_files = [
            "normal_traces.parquet",
            "abnormal_traces.parquet",
            "conclusion.csv",
        ]
        missing_files = [f for f in required_files if not (full_datapack_path / f).exists()]
        if missing_files:
            return datapack_path, False, f"Required files not found: {missing_files}"

        vis_call(full_datapack_path, skip_existing=skip_existing, output_dir=output_dir)
        return datapack_path, True, ""

    except Exception as e:
        return datapack_path, False, str(e)


# ================== Main Visualization Functions ==================


def create_span_visualization(
    entry_df: pl.DataFrame,
    issue_data: list[DtoDetectorRecord],
    datapack_name: str,
    output_file: Path,
    start_time: Any,
    last_normal_time: Any,
) -> None:
    """Create visualization for problematic spans."""
    # Extract span names with issues
    problematic_spans = set()
    for record in issue_data:
        problematic_spans.add(record.span_name)

    if not problematic_spans:
        logger.info(f"No specific problematic spans found in {datapack_name}")
        return

    # Create figure with subplots
    fig, axes = plt.subplots(len(problematic_spans), 1, figsize=(15, 6 * len(problematic_spans)), dpi=300)
    if len(problematic_spans) == 1:
        axes = [axes]

    # Plot each problematic span
    for idx, span_name in enumerate(problematic_spans):
        ax = axes[idx]

        span_data = entry_df.filter(pl.col("SpanName").str.contains(span_name))

        if len(span_data) == 0:
            ax.text(
                0.5,
                0.5,
                f"No data found for {span_name}",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title(f"{span_name}")
            continue

        # Separate normal and abnormal data
        normal_data = span_data.filter(pl.col("trace_type") == "normal")
        abnormal_data = span_data.filter(pl.col("trace_type") == "abnormal")

        # Plot latency over time
        if len(normal_data) > 0:
            normal_times = normal_data.select("datetime").to_numpy().flatten()
            normal_durations = normal_data.select("duration").to_numpy().flatten()
            ax.scatter(
                normal_times, normal_durations, c="green", alpha=0.6, s=PLOT_CONFIG["marker_size"], label="Normal"
            )

        if len(abnormal_data) > 0:
            abnormal_times = abnormal_data.select("datetime").to_numpy().flatten()
            abnormal_durations = abnormal_data.select("duration").to_numpy().flatten()
            ax.scatter(
                abnormal_times, abnormal_durations, c="red", alpha=0.8, s=PLOT_CONFIG["marker_size"], label="Abnormal"
            )

        # Add vertical line to separate normal and abnormal periods
        ax.axvline(x=last_normal_time, color="orange", linestyle="--", alpha=0.7, label="Fault Injection")

        # Formatting
        ax.set_title(f"{span_name}")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (seconds)")
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=PLOT_CONFIG["interval_minutes"]))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    plt.savefig(output_file, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
    plt.close()
    logger.info(f"Visualization saved to {output_file}")


def vis_call(datapack: Path, skip_existing: bool = True, output_dir: Path | None = None) -> None:
    matplotlib.use("Agg")  # Use non-interactive backend

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


@app.command()
def query_issues():
    """Query datapacks with issues."""
    input_path = Path("data") / "rcabench_dataset"
    datapacks, no_issue_datapacks, errs = collect_issues(input_path)
    logger.info(f"Found {len(datapacks)} datapacks with issues, skipping {len(errs)} with errors")
    return datapacks, no_issue_datapacks, errs


@app.command(name="get-dataset-num")
def query_with_confidence(duration: int):
    """Query datapacks with confidence based on duration."""
    input_path = Path("data") / "rcabench_dataset"
    datapack_paths = [datapack for datapack in input_path.iterdir() if datapack.is_dir()]
    tasks = [
        functools.partial(process_datapack_confidence, datapack_path, duration) for datapack_path in datapack_paths
    ]
    cpu = os.cpu_count()
    assert cpu is not None
    results = fmap_threadpool(tasks, parallel=min(cpu, 32))
    datapacks = [result for result in results if result is not None]
    logger.info(f"Found {len(datapacks)} datapacks with all durations < {duration}s")
    return datapacks


@app.command(name="vis-single-entry")
@timeit()
def visualize_latency(datapack: str):
    datapack_path = Path("data") / "rcabench_dataset" / datapack
    if not datapack_path.exists():
        logger.error(f"Datapack not found: {datapack_path}")
        return
    vis_call(datapack_path)


@app.command(name="vis-entry")
@timeit()
def batch_visualize(skip_existing: bool = True, parallel_workers: int | None = None):
    """Batch visualize multiple datapacks using parallel processing."""

    issue, no_issue, _ = collect_issues(Path("data") / "rcabench_dataset")

    if not issue:
        logger.info("No datapacks with issues found")
        return

    logger.info(f"Found {len(issue)} datapacks with issues to process")

    # Determine number of parallel workers
    cpu = os.cpu_count()
    assert cpu is not None
    max_workers = parallel_workers if parallel_workers is not None else min(cpu, 8)

    # Create tasks for parallel processing
    tasks = [
        functools.partial(process_single_datapack_visualization, datapack_path, skip_existing)
        for datapack_path in issue
    ]

    # Process in parallel with progress tracking
    logger.info(f"Processing {len(tasks)} datapacks using {max_workers} parallel workers")
    results = fmap_processpool(tasks, parallel=max_workers)

    # Collect statistics
    successful_count = 0
    failed_count = 0
    failed_datapacks = []

    for datapack_name, success, error_message in results:
        if success:
            successful_count += 1
        else:
            failed_count += 1
            failed_datapacks.append((datapack_name, error_message))
            logger.error(f"Failed to process {datapack_name}: {error_message}")

    # Report results
    logger.info(f"Batch visualization completed: {successful_count} successful, {failed_count} failed")
    if failed_datapacks:
        logger.warning(f"Failed datapacks: {[name for name, _ in failed_datapacks[:5]]}")
        if len(failed_datapacks) > 5:
            logger.warning(f"... and {len(failed_datapacks) - 5} more failed datapacks")


if __name__ == "__main__":
    app()
