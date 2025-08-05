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

from rcabench_platform.v2.cli.main import app, logger, timeit
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


def get_api_issue_details(
    non_empty_issues: pl.DataFrame,
) -> tuple[set[str], set[str], dict[str, set[str]], dict[str, dict[str, Any]]]:
    """Extract detailed API issue information from conclusion data."""
    apis_with_issues: set[str] = set(non_empty_issues["SpanName"].to_list())
    apis_with_success_rate_issues: set[str] = set()
    apis_with_latency_issues: dict[str, set[str]] = {}
    api_issue_details: dict[str, dict[str, Any]] = {}

    for row in non_empty_issues.iter_rows(named=True):
        api_name = row["SpanName"]
        issues_json = row["Issues"]
        if not issues_json:
            continue
        try:
            if isinstance(issues_json, str):
                tmp = json.loads(issues_json)
                if not isinstance(tmp, dict):
                    continue
                issues_dict = cast(dict[str, Any], tmp)
            elif isinstance(issues_json, dict):
                issues_dict = cast(dict[str, Any], issues_json)
            else:
                continue
        except Exception:
            continue

        api_issue_details[api_name] = {}
        latency_issues: set[str] = set()

        for issue_type in [
            "avg_duration",
            "p90_duration",
            "p95_duration",
            "p99_duration",
        ]:
            if issue_type in issues_dict:
                latency_issues.add(issue_type)
                api_issue_details[api_name][issue_type] = issues_dict[issue_type]

        if "succ_rate" in issues_dict:
            apis_with_success_rate_issues.add(api_name)
            api_issue_details[api_name]["succ_rate"] = issues_dict["succ_rate"]

        if latency_issues:
            apis_with_latency_issues[api_name] = latency_issues

    return (
        apis_with_issues,
        apis_with_success_rate_issues,
        apis_with_latency_issues,
        api_issue_details,
    )


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


def filter_valid_apis(
    entry_df: pl.DataFrame,
    apis_with_issues: set[str],
    apis_with_latency_issues: dict[str, set[str]],
    apis_with_success_rate_issues: set[str],
) -> list[tuple[str, pl.DataFrame, pl.DataFrame, set[str], bool]]:
    """Filter and prepare valid APIs for plotting."""
    api_groups = entry_df.group_by("api_path")
    valid_apis: list[tuple[str, pl.DataFrame, pl.DataFrame, set[str], bool]] = []

    for api_path, group_df in api_groups:
        api_name: str = str(api_path[0]) if isinstance(api_path, tuple) else str(api_path)
        if apis_with_issues and api_name not in apis_with_issues:
            continue

        group_df = group_df.sort("datetime")
        normal_data: pl.DataFrame = group_df.filter(pl.col("trace_type") == "normal")
        abnormal_data: pl.DataFrame = group_df.filter(pl.col("trace_type") == "abnormal")

        if len(normal_data) == 0 and len(abnormal_data) == 0:
            continue

        latency_issues: set[str] = apis_with_latency_issues.get(api_name, set())
        has_success_rate_issue: bool = api_name in apis_with_success_rate_issues

        if latency_issues or has_success_rate_issue:
            valid_apis.append(
                (
                    api_name,
                    normal_data,
                    abnormal_data,
                    latency_issues,
                    has_success_rate_issue,
                )
            )

    return valid_apis


# ================== Plotting Functions ==================


def setup_figure(
    valid_apis: list[tuple[str, pl.DataFrame, pl.DataFrame, set[str], bool]],
    datapack_name: str,
) -> tuple[Figure, list[Any]]:
    """Setup the main figure and axes for plotting."""
    total_subplots: int = sum(
        1 for _, _, _, latency_issues, has_success_rate_issue in valid_apis if latency_issues
    ) + sum(1 for _, _, _, _, has_success_rate_issue in valid_apis if has_success_rate_issue)

    fig, axes = plt.subplots(
        total_subplots,
        1,
        figsize=(
            PLOT_CONFIG["figure_size"][0],
            PLOT_CONFIG["figure_size"][1] * total_subplots,
        ),
        sharex=True,
    )
    fig.suptitle(f"Datapack: {datapack_name}", fontsize=16, fontweight="bold")

    if total_subplots == 1:
        axes = [axes]

    return fig, axes


def plot_latency_chart(
    ax,
    api_name: str,
    normal_data: pl.DataFrame,
    abnormal_data: pl.DataFrame,
    last_normal_time,
    latency_issues: set[str],
    api_issue_details: dict[str, dict[str, Any]],
) -> None:
    """Plot latency chart for a specific API."""
    if len(normal_data) > 0:
        ax.plot(
            normal_data["datetime"].to_list(),
            normal_data["duration"].to_list(),
            label="Normal Latency",
            color="blue",
            alpha=PLOT_CONFIG["alpha"],
            linewidth=PLOT_CONFIG["line_width"],
            marker="o",
            markersize=PLOT_CONFIG["marker_size"],
        )

    if len(abnormal_data) > 0:
        ax.plot(
            abnormal_data["datetime"].to_list(),
            abnormal_data["duration"].to_list(),
            label="Abnormal Latency",
            color="red",
            alpha=PLOT_CONFIG["alpha"],
            linewidth=PLOT_CONFIG["line_width"],
            marker="o",
            markersize=PLOT_CONFIG["marker_size"],
        )

    if len(normal_data) > 0 and len(abnormal_data) > 0:
        ax.axvline(
            x=last_normal_time,
            color="black",
            linestyle="-",
            linewidth=2,
            alpha=0.8,
            label="Normal/Abnormal Boundary",
        )

    ax.set_ylabel("Duration (seconds)", fontsize=12)

    # Prepare issue information
    normal_count = len(normal_data)
    abnormal_count = len(abnormal_data)
    issue_info = []

    for issue_type in latency_issues:
        if api_name in api_issue_details and issue_type in api_issue_details[api_name]:
            issue_data = api_issue_details[api_name][issue_type]
            normal_val = issue_data.get("normal", 0)
            abnormal_val = issue_data.get("abnormal", 0)
            change_rate = issue_data.get("change_rate", 0)
            issue_info.append(f"{issue_type}: {normal_val:.3f}s→{abnormal_val:.3f}s (+{change_rate:.1%})")

    title_with_info = (
        f"Request Latency - {api_name}\n"
        f"(Normal: {normal_count} requests, Abnormal: {abnormal_count} requests)\n"
        f"Issues: {', '.join(issue_info)}"
    )
    ax.set_title(title_with_info, fontsize=14, fontweight="bold")
    ax.legend()
    ax.grid(True, alpha=0.3)


def plot_status_code_chart(
    ax,
    api_name: str,
    normal_data: pl.DataFrame,
    abnormal_data: pl.DataFrame,
    last_normal_time,
    api_issue_details: dict[str, dict[str, Any]],
) -> None:
    """Plot status code chart for a specific API."""
    normal_count = len(normal_data)
    abnormal_count = len(abnormal_data)

    if len(normal_data) > 0:
        normal_times = normal_data["datetime"].to_list()
        normal_status_codes = [int(code) if code.isdigit() else 0 for code in normal_data["status_code"].to_list()]
        ax.scatter(
            normal_times,
            normal_status_codes,
            label="Normal Status Code",
            color="blue",
            alpha=0.7,
            s=10,
        )

    if len(abnormal_data) > 0:
        abnormal_times = abnormal_data["datetime"].to_list()
        abnormal_status_codes = [int(code) if code.isdigit() else 0 for code in abnormal_data["status_code"].to_list()]
        ax.scatter(
            abnormal_times,
            abnormal_status_codes,
            label="Abnormal Status Code",
            color="red",
            alpha=0.7,
            s=10,
        )

    if len(normal_data) > 0 and len(abnormal_data) > 0:
        ax.axvline(
            x=last_normal_time,
            color="black",
            linestyle="-",
            linewidth=2,
            alpha=0.8,
            label="Normal/Abnormal Boundary",
        )

    ax.set_ylabel("HTTP Status Code", fontsize=12)

    # Prepare success rate information
    success_rate_info = ""
    if api_name in api_issue_details and "succ_rate" in api_issue_details[api_name]:
        succ_data = api_issue_details[api_name]["succ_rate"]
        normal_rate = succ_data.get("normal", 0)
        abnormal_rate = succ_data.get("abnormal", 0)
        rate_drop = succ_data.get("rate_drop", 0)
        success_rate_info = f"Success Rate: {normal_rate:.1%}→{abnormal_rate:.1%} (-{rate_drop:.1%})"

    status_title_with_info = (
        f"Status Code - {api_name}\n"
        f"(Normal: {normal_count} requests, Abnormal: {abnormal_count} requests)\n"
        f"Issue: {success_rate_info}"
    )
    ax.set_title(status_title_with_info, fontsize=14, fontweight="bold")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_yticks([200, 400, 500])
    ax.set_ylim(150, 550)


def finalize_plot(axes: list[Any]) -> None:
    """Finalize the plot with time formatting and layout."""
    axes[-1].set_xlabel("Time", fontsize=12)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz="Asia/Shanghai"))
    axes[-1].xaxis.set_major_locator(mdates.MinuteLocator(interval=PLOT_CONFIG["interval_minutes"]))
    plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=45)
    plt.tight_layout()


def save_and_show_figure(fig: matplotlib.figure.Figure, output_path: Path, title: str) -> None:
    """Save and show a matplotlib figure."""
    output_path.parent.mkdir(exist_ok=True)
    fig.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
    logger.info(f"Visualization chart saved to: {output_path}")
    plt.show()


# ================== Parallel Processing Functions ==================


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
        # Configure matplotlib for process-safe operation
        import matplotlib

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


def process_single_datapack_patch(
    datapack_name: str,
    skip_existing: bool = True,
    output_dir: Path | None = None,
    base_path: str = "data/rcabench_dataset",
) -> tuple[str, bool, str]:
    """
    Process a single datapack from patch results in a thread-safe manner.

    Returns:
        tuple: (datapack_name, success, error_message)
    """
    try:
        # Configure matplotlib for process-safe operation
        import matplotlib

        matplotlib.use("Agg")  # Use non-interactive backend

        datapack_path = Path(base_path) / datapack_name

        if not datapack_path.exists():
            return datapack_name, False, f"Datapack not found: {datapack_path}"

        required_files = [
            "normal_traces.parquet",
            "abnormal_traces.parquet",
            "conclusion.csv",
        ]
        missing_files = [f for f in required_files if not (datapack_path / f).exists()]
        if missing_files:
            return datapack_name, False, f"Required files not found: {missing_files}"

        vis_call(datapack_path, skip_existing=skip_existing, output_dir=output_dir)
        return datapack_name, True, ""

    except Exception as e:
        return datapack_name, False, str(e)


# ================== Main Visualization Functions ==================


def vis_call(datapack: Path, skip_existing: bool = True, output_dir: Path | None = None) -> None:
    """
    Main visualization function for a single datapack.
    This function has been refactored to be more modular and readable.
    """
    # Ensure matplotlib is configured properly for multi-processing
    import matplotlib

    matplotlib.use("Agg")  # Use non-interactive backend

    # Validate inputs
    conclusion_file: Path = datapack / "conclusion.csv"
    assert conclusion_file.exists()

    # Read and process conclusion data
    conclusion: pl.DataFrame = pl.read_csv(conclusion_file)
    non_empty_issues: pl.DataFrame = conclusion.filter(
        (pl.col("Issues").is_not_null()) & (pl.col("Issues") != "") & (pl.col("Issues") != "{}")
    )

    # Extract API issue details
    (
        apis_with_issues,
        apis_with_success_rate_issues,
        apis_with_latency_issues,
        api_issue_details,
    ) = get_api_issue_details(non_empty_issues)

    # Prepare trace data
    df1, df2, start_time, last_normal_time = prepare_trace_data(datapack)

    # Determine output directory
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
    entry_df = prepare_entry_data(df1, df2)
    if len(entry_df) == 0:
        return

    # Filter valid APIs
    valid_apis = filter_valid_apis(
        entry_df,
        apis_with_issues,
        apis_with_latency_issues,
        apis_with_success_rate_issues,
    )

    if not valid_apis:
        logger.warning(f"{datapack}, No valid APIs found for plotting")
        return

    # Setup figure and plot
    fig, axes = setup_figure(valid_apis, datapack.name)
    current_axis_idx: int = 0

    # Plot each API
    for (
        api_name,
        normal_data,
        abnormal_data,
        latency_issues,
        has_success_rate_issue,
    ) in valid_apis:
        if latency_issues:
            plot_latency_chart(
                axes[current_axis_idx],
                api_name,
                normal_data,
                abnormal_data,
                last_normal_time,
                latency_issues,
                api_issue_details,
            )
            current_axis_idx += 1

        if has_success_rate_issue:
            plot_status_code_chart(
                axes[current_axis_idx],
                api_name,
                normal_data,
                abnormal_data,
                last_normal_time,
                api_issue_details,
            )
            current_axis_idx += 1

    # Finalize and save
    finalize_plot(axes)
    plt.savefig(output_file, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
    plt.close()
    logger.info(f"Saved {len(valid_apis)} APIs to {output_file}")


# ================== Rules Analysis Functions ==================


def create_rule_count_chart(ax, df: pl.DataFrame, rule_columns: list[str], colors) -> None:
    """Create rule count bar chart."""
    rule_counts = []
    rule_labels = []
    for rule in rule_columns:
        count = df.filter(pl.col(rule)).height
        rule_counts.append(count)
        rule_labels.append(rule.replace("rule_", "R").replace("_", " ").title())

    bars = ax.bar(range(len(rule_counts)), rule_counts, color=colors)
    ax.set_xlabel("Rules")
    ax.set_ylabel("Number of Filtered Datasets")
    ax.set_title("Datasets Filtered by Each Rule")
    ax.set_xticks(range(len(rule_labels)))
    ax.set_xticklabels(rule_labels, rotation=45, ha="right")

    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height + max(rule_counts) * 0.01,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )


def create_rules_per_dataset_histogram(ax, df: pl.DataFrame, rule_columns: list[str]) -> None:
    """Create histogram of rules per dataset."""
    rules_per_dataset = df.select([pl.col(rule).cast(pl.Int32) for rule in rule_columns]).sum_horizontal()
    rules_counts = rules_per_dataset.to_numpy()
    ax.hist(rules_counts, bins=range(8), alpha=0.7, color="skyblue", edgecolor="black")
    ax.set_xlabel("Number of Rules Triggered per Dataset")
    ax.set_ylabel("Number of Datasets")
    ax.set_title("Distribution of Rules per Dataset")
    ax.set_xticks(range(7))


def create_correlation_matrix(ax, df: pl.DataFrame, rule_columns: list[str]) -> None:
    """Create rule correlation matrix heatmap."""
    rule_matrix = df.select([pl.col(rule).cast(pl.Int32) for rule in rule_columns]).to_numpy()
    correlation_matrix = np.corrcoef(rule_matrix.T)
    im = ax.imshow(correlation_matrix, cmap="coolwarm", aspect="auto", vmin=-1, vmax=1)
    ax.set_title("Rule Correlation Matrix")
    ax.set_xticks(range(len(rule_columns)))
    ax.set_yticks(range(len(rule_columns)))
    ax.set_xticklabels([f"R{i + 1}" for i in range(len(rule_columns))], rotation=45)
    ax.set_yticklabels([f"R{i + 1}" for i in range(len(rule_columns))])

    for i in range(len(rule_columns)):
        for j in range(len(rule_columns)):
            ax.text(
                j,
                i,
                f"{correlation_matrix[i, j]:.2f}",
                ha="center",
                va="center",
                color="black" if abs(correlation_matrix[i, j]) < 0.5 else "white",
            )

    cbar = plt.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label("Correlation Coefficient")


def create_cumulative_filtering_chart(ax, df: pl.DataFrame, rule_columns: list[str]) -> None:
    """Create cumulative filtering effect chart."""
    import matplotlib.cm as cm

    total_datasets = len(df)
    cumulative_filtered = []
    remaining = total_datasets

    for i, rule in enumerate(rule_columns):
        if i == 0:
            filtered = df.filter(pl.col(rule)).height
        else:
            prev_mask = pl.any_horizontal([pl.col(r) for r in rule_columns[:i]])
            current_mask = pl.any_horizontal([pl.col(r) for r in rule_columns[: i + 1]])
            filtered = df.filter(current_mask & ~prev_mask).height
        cumulative_filtered.append(filtered)
        remaining -= filtered

    cumulative_filtered.append(remaining)
    labels = [f"R{i + 1}" for i in range(len(rule_columns))] + ["Remaining"]
    colors = cm.get_cmap("tab10")(np.linspace(0, 1, len(labels)))

    ax.bar(["Datasets"], [total_datasets], color="lightgray", alpha=0.3, label="Total")
    bottom = 0
    for i, (count, label, color) in enumerate(zip(cumulative_filtered, labels, colors)):
        ax.bar(["Datasets"], [count], bottom=bottom, color=color, label=label, alpha=0.8)
        if count > 0:
            ax.text(
                0,
                bottom + count / 2,
                f"{count}",
                ha="center",
                va="center",
                fontweight="bold",
            )
        bottom += count

    ax.set_ylabel("Number of Datasets")
    ax.set_title("Cumulative Filtering Effect")
    ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")


def __create_visualizations(df: pl.DataFrame, rule_columns: list[str]):
    """Create comprehensive rules analysis visualizations."""
    plt.style.use("default")
    import matplotlib.cm as cm

    colors = cm.get_cmap("tab10")(np.linspace(0, 1, len(rule_columns)))
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle("Rules Check Distribution Analysis", fontsize=16, fontweight="bold")

    # Create individual charts
    create_rule_count_chart(axes[0, 0], df, rule_columns, colors)
    create_rules_per_dataset_histogram(axes[0, 1], df, rule_columns)
    create_correlation_matrix(axes[1, 0], df, rule_columns)
    create_cumulative_filtering_chart(axes[1, 1], df, rule_columns)

    plt.tight_layout()
    output_path = Path("temp/rules_analysis.png")
    output_path.parent.mkdir(exist_ok=True)
    plt.savefig(output_path, dpi=PLOT_CONFIG["dpi"], bbox_inches="tight")
    logger.info(f"Visualization chart saved to: {output_path}")
    plt.show()


def create_category_pie_chart(ax, datapack_categories: dict[str, list[str]], config: dict[str, Any]) -> None:
    """Create pie chart for category distribution."""
    categories = config["categories"]
    counts = [len(datapack_categories[cat]) for cat in categories]
    display_labels = config["display_labels"]
    colors = config["colors"]

    non_abs_idx = [i for i, cat in enumerate(categories) if cat != "absolute_anomaly"]
    non_abs_labels = [display_labels[i] for i in non_abs_idx]
    non_abs_counts = [counts[i] for i in non_abs_idx]
    non_abs_colors = [colors[i] for i in non_abs_idx]

    non_zero_data = [
        (label, count, color)
        for label, count, color in zip(non_abs_labels, non_abs_counts, non_abs_colors)
        if count > 0
    ]

    if non_zero_data:
        labels, values, colors_filtered = zip(*non_zero_data)
        wedges, texts, autotexts = ax.pie(
            values,
            labels=labels,
            autopct="%1.1f%%",
            colors=colors_filtered,
            startangle=90,
        )
        for autotext in autotexts:
            autotext.set_color("white")
            autotext.set_fontweight("bold")

    ax.set_title("Datapack Category Proportions")


def create_category_bar_chart(ax, datapack_categories: dict[str, list[str]], config: dict[str, Any]) -> None:
    """Create bar chart for category statistics."""
    categories = config["categories"]
    counts = [len(datapack_categories[cat]) for cat in categories]
    display_labels = config["display_labels"]
    colors = config["colors"]

    bars = ax.bar(display_labels, counts, color=colors)
    ax.set_title("Datapack Category Statistics")
    ax.set_ylabel("Number of Datapacks")
    ax.tick_params(axis="x", rotation=45)

    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + max(counts) * 0.01,
                f"{int(height)}",
                ha="center",
                va="bottom",
                fontweight="bold",
            )


# ================== Command Line Interface Functions ==================


@app.command()
def run() -> None:
    """Run rules check visualization analysis."""
    rules_check_file = get_dataset_meta_file("rcabench", "rules_check.parquet")
    if not Path(rules_check_file).exists():
        print(f"Error: File does not exist {rules_check_file}")
        return
    df = pl.read_parquet(rules_check_file)
    print(f"Read rule check results for {len(df)} data packages")
    __create_visualizations(df, RULE_COLUMNS)


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
    """Visualize a single datapack."""
    datapack_path = Path("data") / "rcabench_dataset" / datapack
    if not datapack_path.exists():
        logger.error(f"Datapack not found: {datapack_path}")
        return

    required_files = ["normal_traces.parquet", "abnormal_traces.parquet"]
    missing_files = [f for f in required_files if not (datapack_path / f).exists()]
    if missing_files:
        logger.error(f"Required files not found in {datapack_path}: {missing_files}")
        return

    logger.info(f"Starting visualization for datapack: {datapack}")
    vis_call(datapack_path)
    logger.info(f"Visualization completed for datapack: {datapack}")


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


@app.command(name="vis-patch-results")
@timeit()
def visualize_patch_results(
    patch_file: str = "temp/patch_results.txt",
    skip_existing: bool = True,
    parallel_workers: int | None = None,
):
    """Visualize datapacks listed in patch_results.txt file using parallel processing."""

    patch_file_path = Path(patch_file)
    if not patch_file_path.exists():
        logger.error(f"Patch results file not found: {patch_file_path}")
        return

    # Read datapack names from the file
    datapack_names = []
    with open(patch_file_path) as f:
        for line in f:
            line = line.strip()
            if line:  # Skip empty lines
                datapack_names.append(line)

    logger.info(f"Found {len(datapack_names)} datapacks in {patch_file}")

    if not datapack_names:
        logger.info("No datapacks to process")
        return

    # Set up output directory
    output_dir = Path("temp") / "latency_only"
    output_dir.mkdir(parents=True, exist_ok=True)
    for file in output_dir.glob("*.png"):
        file.unlink()

    # Determine number of parallel workers
    cpu = os.cpu_count()
    assert cpu is not None
    max_workers = parallel_workers if parallel_workers is not None else min(cpu, 8)

    # Create tasks for parallel processing
    tasks = [
        functools.partial(process_single_datapack_patch, datapack_name, skip_existing, output_dir)
        for datapack_name in datapack_names
    ]

    # Process in parallel
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
    logger.info(f"Visualization completed: {successful_count} successful, {failed_count} failed")
    logger.info(f"Results saved to: {output_dir}")

    if failed_datapacks:
        logger.warning(f"Failed datapacks: {[name for name, _ in failed_datapacks[:5]]}")
        if len(failed_datapacks) > 5:
            logger.warning(f"... and {len(failed_datapacks) - 5} more failed datapacks")


if __name__ == "__main__":
    app()
