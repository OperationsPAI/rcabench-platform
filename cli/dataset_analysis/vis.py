#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.datasets.spec import get_dataset_meta_file
import polars as pl
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from zoneinfo import ZoneInfo
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.utils.fmap import fmap_processpool, fmap_threadpool
from rcabench_platform.v2.datasets.train_ticket import extract_path
import json
import os
import functools
import matplotlib.dates as mdates
from typing import Any, cast
from collections.abc import Generator
import matplotlib.figure

RULE_COLUMNS = [
    "rule_1_network_no_direct_calls",
    "rule_2_http_method_same",
    "rule_3_http_no_direct_calls",
    "rule_4_single_point_no_calls",
    "rule_5_duplicated_spans",
    "rule_6_large_latency_normal",
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


def iter_datapacks(input_path: Path, file_name: str) -> Generator[tuple[Path, Path], None, None]:
    for datapack in input_path.iterdir():
        if not datapack.is_dir():
            continue
        file = datapack / file_name
        if not file.exists():
            logger.warning(f"No {file_name} found for {datapack.name}, skipping")
            continue
        yield datapack, file


def read_json(file: Path) -> dict[str, Any]:
    with open(file, encoding="utf-8") as f:
        return json.load(f)


def classify_issue_categories_multi(data: dict[str, Any]) -> list[str]:
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


def save_and_show_figure(fig: matplotlib.figure.Figure, output_path: Path, title: str) -> None:
    output_path.parent.mkdir(exist_ok=True)
    fig.suptitle(title, fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    logger.info(f"Visualization chart saved to: {output_path}")
    plt.show()


def read_dataframe(file: Path) -> pl.LazyFrame:
    return pl.scan_parquet(file)


def collect_issues(input_path: Path) -> tuple[list[str], list[str], list[tuple[str, str]]]:
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


def extract_status_code(span_attributes: str) -> str:
    try:
        ra = json.loads(span_attributes) if span_attributes else {}
        return ra["http.status_code"]
    except Exception:
        return "-1"


def get_api_issue_details(
    non_empty_issues: pl.DataFrame,
) -> tuple[set[str], set[str], dict[str, set[str]], dict[str, dict[str, Any]]]:
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
        for issue_type in ["avg_duration", "p90_duration", "p95_duration", "p99_duration"]:
            if issue_type in issues_dict:
                latency_issues.add(issue_type)
                api_issue_details[api_name][issue_type] = issues_dict[issue_type]  # type: ignore
        if "succ_rate" in issues_dict:
            apis_with_success_rate_issues.add(api_name)
            api_issue_details[api_name]["succ_rate"] = issues_dict["succ_rate"]  # type: ignore
        if latency_issues:
            apis_with_latency_issues[api_name] = latency_issues
    return apis_with_issues, apis_with_success_rate_issues, apis_with_latency_issues, api_issue_details


def vis_call(datapack: Path, skip_existing: bool = True) -> None:
    conclusion_file: Path = datapack / "conclusion.csv"
    assert conclusion_file.exists()
    conclusion: pl.DataFrame = pl.read_csv(conclusion_file)
    non_empty_issues: pl.DataFrame = conclusion.filter(
        (pl.col("Issues").is_not_null()) & (pl.col("Issues") != "") & (pl.col("Issues") != "{}")
    )
    apis_with_issues: set[str]
    apis_with_success_rate_issues: set[str]
    apis_with_latency_issues: dict[str, set[str]]
    api_issue_details: dict[str, dict[str, Any]]
    apis_with_issues, apis_with_success_rate_issues, apis_with_latency_issues, api_issue_details = (
        get_api_issue_details(non_empty_issues)
    )
    df1: pl.DataFrame = pl.scan_parquet(datapack / "normal_traces.parquet").collect()
    df2: pl.DataFrame = pl.scan_parquet(datapack / "abnormal_traces.parquet").collect()
    start_time = df1.select(pl.col("Timestamp").min()).item()
    last_normal_time = df1.select(pl.col("Timestamp").max()).item()
    hour_key: str = start_time.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d_%H")
    output_dir: Path = Path("temp") / "vis_by_hour" / hour_key
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file: Path = output_dir / f"{datapack.name}.png"
    if output_file.exists() and skip_existing:
        return
    df1 = df1.with_columns(pl.lit("normal").alias("trace_type"))
    df2 = df2.with_columns(pl.lit("abnormal").alias("trace_type"))
    merged_df: pl.DataFrame = pl.concat([df1, df2])
    entry_df: pl.DataFrame = merged_df.filter(
        (pl.col("ServiceName") == "loadgenerator-service")
        & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
    )
    if len(entry_df) == 0:
        logger.error("loadgenerator-service not found in trace data, using ts-ui-dashboard as fallback")
        entry_df = merged_df.filter(
            (pl.col("ServiceName") == "ts-ui-dashboard")
            & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
        )
    if len(entry_df) == 0:
        logger.error("No valid entrypoint found in trace data")
        return
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
    api_groups = entry_df.group_by("api_path")
    valid_apis: list[tuple[str, pl.DataFrame, pl.DataFrame, set[str], bool]] = []
    for api_path, group_df in api_groups:
        api_name: str = str(api_path[0]) if isinstance(api_path, tuple) else str(api_path)
        if apis_with_issues and api_name not in apis_with_issues:
            continue
        if len(group_df) < 10:
            continue
        group_df = group_df.sort("datetime")
        normal_data: pl.DataFrame = group_df.filter(pl.col("trace_type") == "normal")
        abnormal_data: pl.DataFrame = group_df.filter(pl.col("trace_type") == "abnormal")
        if len(normal_data) == 0 and len(abnormal_data) == 0:
            continue
        latency_issues: set[str] = set()
        if isinstance(apis_with_latency_issues, dict):
            latency_issues = apis_with_latency_issues.get(api_name, set())
        has_success_rate_issue: bool = api_name in apis_with_success_rate_issues
        if latency_issues or has_success_rate_issue:
            valid_apis.append((api_name, normal_data, abnormal_data, latency_issues, has_success_rate_issue))
    if not valid_apis:
        logger.warning("No valid APIs found for plotting")
        return
    total_subplots: int = sum(
        1 for _, _, _, latency_issues, has_success_rate_issue in valid_apis if latency_issues
    ) + sum(1 for _, _, _, _, has_success_rate_issue in valid_apis if has_success_rate_issue)
    fig, axes = plt.subplots(total_subplots, 1, figsize=(15, 4 * total_subplots), sharex=True)
    fig.suptitle(f"Datapack: {datapack.name}", fontsize=16, fontweight="bold")
    if total_subplots == 1:
        axes = [axes]
    interval_minutes: int = 1
    current_axis_idx: int = 0
    for api_name, normal_data, abnormal_data, latency_issues, has_success_rate_issue in valid_apis:
        if latency_issues:
            ax = axes[current_axis_idx]
            current_axis_idx += 1
            if len(normal_data) > 0:
                ax.plot(
                    normal_data["datetime"].to_list(),
                    normal_data["duration"].to_list(),
                    label="Normal Latency",
                    color="blue",
                    alpha=0.7,
                    linewidth=0.8,
                    marker="o",
                    markersize=1,
                )
            if len(abnormal_data) > 0:
                ax.plot(
                    abnormal_data["datetime"].to_list(),
                    abnormal_data["duration"].to_list(),
                    label="Abnormal Latency",
                    color="red",
                    alpha=0.7,
                    linewidth=0.8,
                    marker="o",
                    markersize=1,
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
        if has_success_rate_issue:
            status_ax = axes[current_axis_idx]
            current_axis_idx += 1
            normal_count = len(normal_data)
            abnormal_count = len(abnormal_data)
            if len(normal_data) > 0:
                normal_times = normal_data["datetime"].to_list()
                normal_status_codes = [
                    int(code) if code.isdigit() else 0 for code in normal_data["status_code"].to_list()
                ]
                status_ax.scatter(
                    normal_times, normal_status_codes, label="Normal Status Code", color="blue", alpha=0.7, s=10
                )
            if len(abnormal_data) > 0:
                abnormal_times = abnormal_data["datetime"].to_list()
                abnormal_status_codes = [
                    int(code) if code.isdigit() else 0 for code in abnormal_data["status_code"].to_list()
                ]
                status_ax.scatter(
                    abnormal_times, abnormal_status_codes, label="Abnormal Status Code", color="red", alpha=0.7, s=10
                )
            if len(normal_data) > 0 and len(abnormal_data) > 0:
                status_ax.axvline(
                    x=last_normal_time,
                    color="black",
                    linestyle="-",
                    linewidth=2,
                    alpha=0.8,
                    label="Normal/Abnormal Boundary",
                )
            status_ax.set_ylabel("HTTP Status Code", fontsize=12)
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
            status_ax.set_title(status_title_with_info, fontsize=14, fontweight="bold")
            status_ax.legend()
            status_ax.grid(True, alpha=0.3)
            status_ax.set_yticks([200, 400, 500])
            status_ax.set_ylim(150, 550)
    axes[-1].set_xlabel("Time", fontsize=12)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz="Asia/Shanghai"))
    axes[-1].xaxis.set_major_locator(mdates.MinuteLocator(interval=interval_minutes))
    plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=45)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved {len(valid_apis)} APIs to {output_file}")


# =====================
# 命令函数
# =====================


@app.command()
def run() -> None:
    rules_check_file = get_dataset_meta_file("rcabench", "rules_check.parquet")
    if not Path(rules_check_file).exists():
        print(f"Error: File does not exist {rules_check_file}")
        return
    df = pl.read_parquet(rules_check_file)
    print(f"Read rule check results for {len(df)} data packages")
    __create_visualizations(df, RULE_COLUMNS)


def __create_visualizations(df: pl.DataFrame, rule_columns: list[str]):
    plt.style.use("default")
    import matplotlib.cm as cm

    colors = cm.get_cmap("tab10")(np.linspace(0, 1, len(rule_columns)))
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle("Rules Check Distribution Analysis", fontsize=16, fontweight="bold")
    ax1 = axes[0, 0]
    rule_counts = []
    rule_labels = []
    for rule in rule_columns:
        count = df.filter(pl.col(rule)).height
        rule_counts.append(count)
        rule_labels.append(rule.replace("rule_", "R").replace("_", " ").title())
    bars = ax1.bar(range(len(rule_counts)), rule_counts, color=colors)
    ax1.set_xlabel("Rules")
    ax1.set_ylabel("Number of Filtered Datasets")
    ax1.set_title("Datasets Filtered by Each Rule")
    ax1.set_xticks(range(len(rule_labels)))
    ax1.set_xticklabels(rule_labels, rotation=45, ha="right")
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width() / 2.0,
            height + max(rule_counts) * 0.01,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )
    ax2 = axes[0, 1]
    rules_per_dataset = df.select([pl.col(rule).cast(pl.Int32) for rule in rule_columns]).sum_horizontal()
    rules_counts = rules_per_dataset.to_numpy()
    ax2.hist(rules_counts, bins=range(8), alpha=0.7, color="skyblue", edgecolor="black")
    ax2.set_xlabel("Number of Rules Triggered per Dataset")
    ax2.set_ylabel("Number of Datasets")
    ax2.set_title("Distribution of Rules per Dataset")
    ax2.set_xticks(range(7))
    ax3 = axes[1, 0]
    rule_matrix = df.select([pl.col(rule).cast(pl.Int32) for rule in rule_columns]).to_numpy()
    correlation_matrix = np.corrcoef(rule_matrix.T)
    im = ax3.imshow(correlation_matrix, cmap="coolwarm", aspect="auto", vmin=-1, vmax=1)
    ax3.set_title("Rule Correlation Matrix")
    ax3.set_xticks(range(len(rule_columns)))
    ax3.set_yticks(range(len(rule_columns)))
    ax3.set_xticklabels([f"R{i + 1}" for i in range(len(rule_columns))], rotation=45)
    ax3.set_yticklabels([f"R{i + 1}" for i in range(len(rule_columns))])
    for i in range(len(rule_columns)):
        for j in range(len(rule_columns)):
            ax3.text(
                j,
                i,
                f"{correlation_matrix[i, j]:.2f}",
                ha="center",
                va="center",
                color="black" if abs(correlation_matrix[i, j]) < 0.5 else "white",
            )
    cbar = plt.colorbar(im, ax=ax3, shrink=0.8)
    cbar.set_label("Correlation Coefficient")
    ax4 = axes[1, 1]
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
    ax4.bar(["Datasets"], [total_datasets], color="lightgray", alpha=0.3, label="Total")
    bottom = 0
    for i, (count, label, color) in enumerate(zip(cumulative_filtered, labels, colors)):
        ax4.bar(["Datasets"], [count], bottom=bottom, color=color, label=label, alpha=0.8)
        if count > 0:
            ax4.text(0, bottom + count / 2, f"{count}", ha="center", va="center", fontweight="bold")
        bottom += count
    ax4.set_ylabel("Number of Datasets")
    ax4.set_title("Cumulative Filtering Effect")
    ax4.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    output_path = Path("temp/rules_analysis.png")
    output_path.parent.mkdir(exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    logger.info(f"Visualization chart saved to: {output_path}")
    plt.show()


@app.command()
def query_issues():
    input_path = Path("data") / "rcabench_dataset"
    datapacks, no_issue_datapacks, errs = collect_issues(input_path)
    logger.info(f"Found {len(datapacks)} datapacks with issues, skipping {len(errs)} with errors")
    return datapacks, no_issue_datapacks, errs


@app.command()
def query_with_confidence(duration: int):
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


@app.command()
@timeit()
def visualize_latency(datapack: str):
    datapack_path = Path("data") / "rcabench_dataset" / datapack
    if not datapack_path.exists():
        logger.error(f"Datapack not found: {datapack_path}")
        return
    normal_traces = datapack_path / "normal_traces.parquet"
    abnormal_traces = datapack_path / "abnormal_traces.parquet"
    if not normal_traces.exists() or not abnormal_traces.exists():
        logger.error(f"Required trace files not found in {datapack_path}")
        return
    logger.info(f"Starting visualization for datapack: {datapack}")
    vis_call(datapack_path)
    logger.info(f"Visualization completed for datapack: {datapack}")


@app.command(name="batch-vis")
@timeit()
def batch_visualize(skip_existing: bool = True):
    from tqdm import tqdm

    issue, no_issue, _ = collect_issues(Path("data") / "rcabench_dataset")
    for datapack_path in tqdm(issue):
        vis_call(Path("data/rcabench_dataset") / datapack_path, skip_existing=skip_existing)
    logger.info("Batch visualization completed")


@app.command(name="vis-anno")
@timeit()
def visualize_annotations():
    from tqdm import tqdm

    input_path = Path("data") / "rcabench_dataset"
    # 新增 absolute_anomaly 独立计数
    datapack_categories = {cat: [] for cat in CATEGORY_CONFIG["categories"]}
    absolute_anomaly_datapacks = []
    valid_datapacks = 0
    errors = []
    logger.info("Starting to collect notations.json data...")
    for datapack, notations_file in iter_datapacks(input_path, "notations.json"):
        try:
            data = read_json(notations_file)
            cats = classify_issue_categories_multi(data)
            for cat in cats:
                if cat == "absolute_anomaly":
                    absolute_anomaly_datapacks.append(datapack.name)
                if cat in datapack_categories:
                    datapack_categories[cat].append(datapack.name)
            valid_datapacks += 1
        except Exception as e:
            logger.error(f"Error processing {notations_file}: {e}")
            errors.append((datapack.name, str(e)))
    logger.info("=== Debug: Category counts ===")
    for category, datapacks in datapack_categories.items():
        logger.info(f"{category}: {len(datapacks)} datapacks")
        if category == "absolute_anomaly" and datapacks:
            logger.info(f"  Absolute anomaly datapacks: {', '.join(datapacks[:5])}")
    logger.info(f"Successfully processed {valid_datapacks} datapacks, {len(errors)} errors")
    if valid_datapacks == 0:
        logger.error("No valid notations.json files found")
        return
    plt.style.use("default")
    categories = CATEGORY_CONFIG["categories"]
    counts = [len(datapack_categories[cat]) for cat in categories]
    display_labels = CATEGORY_CONFIG["display_labels"]
    colors = CATEGORY_CONFIG["colors"]
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Datapack Issue Categories Distribution", fontsize=16, fontweight="bold")
    ax1, ax2 = axes

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
        wedges, texts, autotexts = ax1.pie(
            values, labels=labels, autopct="%1.1f%%", colors=colors_filtered, startangle=90
        )
        for autotext in autotexts:
            autotext.set_color("white")
            autotext.set_fontweight("bold")
    ax1.set_title("Datapack Category Proportions (不含绝对异常)")

    bars = ax2.bar(display_labels, counts, color=colors)
    ax2.set_title("Datapack Category Statistics")
    ax2.set_ylabel("Number of Datapacks")
    ax2.tick_params(axis="x", rotation=45)
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax2.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + max(counts) * 0.01,
                f"{int(height)}",
                ha="center",
                va="bottom",
                fontweight="bold",
            )
    # absolute_anomaly 单独柱状图

    plt.tight_layout()
    output_path = Path("temp/datapack_categories.png")
    output_path.parent.mkdir(exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()
    logger.info(f"Visualization chart saved to: {output_path}")


if __name__ == "__main__":
    app()
