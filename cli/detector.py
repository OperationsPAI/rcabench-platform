#!/usr/bin/env -S uv run -s
import functools
import json
import os
import sys
from pathlib import Path
from typing import Any, Literal, TypedDict

import numpy as np
import polars as pl

from rcabench_platform.v2.cli.main import app
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.logging import logger, timeit
from rcabench_platform.v2.metrics.ad.configs import (
    EnhancedLatencyConfig,
    SuccessRateConfig,
)
from rcabench_platform.v2.metrics.ad.detectors import (
    EnhancedLatencyDetector,
    SuccessRateDetector,
)
from rcabench_platform.v2.metrics.ad.types import HistoricalData
from rcabench_platform.v2.utils.fmap import fmap_processpool


# TypedDict definitions
class ThresholdInfo(TypedDict, total=False):
    """Threshold information"""

    rule_based_anomaly: bool
    p_value: float
    z_statistic: float


class AnomalyScoreResult(TypedDict):
    """Anomaly score result"""

    is_anomaly: bool
    total_score: float
    change_rate: float
    abnormal_value: float
    absolute_change: float
    description: str
    severity: str
    detection_method: str
    threshold_info: dict[str, Any] | None
    rule_anomaly: bool


class SuccessRateResult(TypedDict):
    """Success rate detection result"""

    is_significant: bool
    p_value: float
    z_statistic: float
    change_rate: float
    rate_drop: float
    confidence: float
    description: str
    severity: str


class ConclusionRowResult(TypedDict):
    """Conclusion detection result"""

    # Latency results
    latency_is_anomaly: bool
    latency_total_score: float
    latency_change_rate: float
    latency_abnormal_value: float
    latency_absolute_change: float
    latency_description: str
    latency_severity: str
    latency_detection_method: str
    latency_threshold_info: dict[str, Any] | None

    # Success rate results
    success_rate_is_significant: bool
    success_rate_p_value: float
    success_rate_z_statistic: float
    success_rate_change_rate: float
    success_rate_rate_drop: float
    success_rate_confidence: float
    success_rate_description: str


class IssueCategories(TypedDict):
    """Issue category statistics"""

    latency_only: int
    success_rate_only: int
    both_latency_and_success_rate: int
    no_issues: int


class Notations(TypedDict):
    """Analysis notation information"""

    issue_categories: IssueCategories
    total_endpoints: int
    skipped_endpoints: int
    absolute_anomaly: bool
    anomaly_count: int


class ConclusionRow(TypedDict):
    """Conclusion row data"""

    SpanName: str
    Issues: str
    AbnormalAvgDuration: float
    NormalAvgDuration: float
    AbnormalSuccRate: float
    NormalSuccRate: float
    AbnormalP90: float
    NormalP90: float
    AbnormalP95: float
    NormalP95: float
    AbnormalP99: float
    NormalP99: float


class AnalysisState(TypedDict):
    """Analysis state"""

    conclusion_data: list[ConclusionRow]
    anomaly_count: int
    processed_endpoints: int
    skipped_endpoints: int
    notations: Notations


class EndpointStats(TypedDict):
    """Endpoint statistics data"""

    timestamp: list[int]
    duration: list[int]
    status_code: list[int | str]
    response_content_length: list[int]
    request_content_length: list[int]
    avg_duration: float | None
    p90_duration: float | None
    p95_duration: float | None
    p99_duration: float | None
    succ_rate: float | None


class AnalysisResult(TypedDict):
    """Analysis result"""

    datapack_name: str
    is_latency_only: bool
    total_endpoints: int
    anomaly_count: int
    issue_categories: IssueCategories
    absolute_anomaly: bool


def calculate_anomaly_score(
    tp: Literal["avg", "p90", "p95", "p99"],
    normal_data: list[float],
    abnormal_value: float,
) -> AnomalyScoreResult:
    detector = EnhancedLatencyDetector()
    config = EnhancedLatencyConfig(
        percentile_type=tp,
    )

    historical_data: HistoricalData = {"values": normal_data, "timestamps": None}

    result = detector.detect(abnormal_value, historical_data, config)

    normal_mean = np.mean(normal_data) if normal_data else 0.0
    change_rate = (abnormal_value - normal_mean) / normal_mean if normal_mean > 0 else 0.0
    absolute_change = abnormal_value - normal_mean

    # Check if it's a rule-based anomaly (hard timeout or adaptive rules)
    rule_anomaly = False
    if result["threshold_info"] and result["threshold_info"].get("rule_based_anomaly"):
        rule_anomaly = True
    elif abnormal_value > config.hard_timeout_threshold:
        rule_anomaly = True

    return_dict: AnomalyScoreResult = {
        "is_anomaly": result["is_anomaly"],
        "total_score": float(result["confidence"]),
        "change_rate": float(change_rate),
        "abnormal_value": float(abnormal_value),
        "absolute_change": float(absolute_change),
        "description": result["description"],
        "severity": result["severity"],
        "detection_method": result["detection_method"],
        "threshold_info": result["threshold_info"],
        "rule_anomaly": rule_anomaly,
    }

    return return_dict


def is_success_rate_significant(
    normal_succ_rate: float,
    abnormal_succ_rate: float,
    normal_total: int,
    abnormal_total: int,
) -> SuccessRateResult:
    detector = SuccessRateDetector()
    config = SuccessRateConfig(
        enabled=True,
        min_normal_count=10,
        min_abnormal_count=5,
        min_rate_drop=0.03,
        significance_threshold=0.05,
        min_relative_drop=0.1,
    )

    historical_data: HistoricalData = {"values": [], "timestamps": None}

    result = detector.detect(
        current_value=abnormal_succ_rate,
        historical_data=historical_data,
        config=config,
        normal_rate=normal_succ_rate,
        abnormal_rate=abnormal_succ_rate,
        normal_count=normal_total,
        abnormal_count=abnormal_total,
    )

    rate_drop = normal_succ_rate - abnormal_succ_rate
    change_rate = rate_drop / normal_succ_rate if normal_succ_rate > 0 else 0.0

    p_value = 1.0
    z_statistic = 0.0
    if result["threshold_info"]:
        p_value = result["threshold_info"].get("p_value", 1.0)
        z_statistic = result["threshold_info"].get("z_statistic", 0.0)

    return_result: SuccessRateResult = {
        "is_significant": result["is_anomaly"],
        "p_value": float(p_value),
        "z_statistic": float(z_statistic),
        "change_rate": float(change_rate),
        "rate_drop": float(rate_drop),
        "confidence": float(result["confidence"]),
        "description": result["description"],
        "severity": result["severity"],
    }
    return return_result


def read_dataframe(file: Path) -> pl.LazyFrame:
    return pl.scan_parquet(file)


def preprocess_trace(file: Path) -> dict[str, Any]:
    if not file.exists():
        logger.error(f"Trace file does not exist: {file}")
        return {}

    df = read_dataframe(file)

    entry_df = df.filter(
        (pl.col("ServiceName") == "loadgenerator") & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
    )

    entry_count = entry_df.select(pl.len()).collect().item()
    if entry_count == 0:
        logger.error("loadgenerator not found in trace data, using ts-ui-dashboard as fallback")
        entry_df = df.filter(
            (pl.col("ServiceName") == "ts-ui-dashboard")
            & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
        )
        entry_count = entry_df.select(pl.len()).collect().item()

    if entry_count == 0:
        logger.error("No valid entrypoint found in trace data, aborting")

        available_services = df.select(pl.col("ServiceName")).unique().collect()["ServiceName"].to_list()
        logger.error(f"Available services in trace data: {available_services}")

        return {}

    entry_df_collected = entry_df.with_columns(pl.col("Timestamp").alias("ts")).sort("ts").collect()

    entrypoints = set(entry_df_collected["SpanName"].to_list())

    deduped_entrypoints = {}
    for entrypoint in entrypoints:
        path = extract_path(entrypoint)
        deduped_entrypoints[entrypoint] = path

    stat = {}

    span_groups = entry_df_collected.group_by("SpanName")

    for span_name, group_df in span_groups:
        dedupe_name = deduped_entrypoints.get(span_name[0], span_name[0])

        if dedupe_name not in stat:
            stat[dedupe_name] = {
                "timestamp": [],
                "duration": [],
                "status_code": [],
                "response_content_length": [],
                "request_content_length": [],
            }

        timestamps = group_df["Timestamp"].to_list()
        durations = group_df["Duration"].to_list()

        stat[dedupe_name]["timestamp"].extend(timestamps)
        stat[dedupe_name]["duration"].extend(durations)

        for row in group_df.iter_rows(named=True):
            ra = json.loads(row["SpanAttributes"])
            if "http.status_code" in ra:
                stat[dedupe_name]["status_code"].append(ra["http.status_code"])
            elif row["StatusCode"] != "Unset":
                stat[dedupe_name]["status_code"].append(row["StatusCode"])

            if "http.response_content_length" in ra:
                stat[dedupe_name]["response_content_length"].append(ra["http.response_content_length"])
            if "http.request_content_length" in ra:
                stat[dedupe_name]["request_content_length"].append(ra["http.request_content_length"])

    for k, v in stat.items():
        durations = v["duration"]
        if not durations:
            logger.warning(f"No duration data found for endpoint: {k}")
            # Set duration metrics to None when no data is available
            v["avg_duration"] = None
            v["p90_duration"] = None
            v["p95_duration"] = None
            v["p99_duration"] = None
        else:
            durations_array = np.array(durations)
            avg_duration = np.mean(durations_array)
            p90_duration = np.percentile(durations_array, 90)
            p95_duration = np.percentile(durations_array, 95)
            p99_duration = np.percentile(durations_array, 99)

            v["avg_duration"] = avg_duration / 1e9
            v["p90_duration"] = p90_duration / 1e9
            v["p95_duration"] = p95_duration / 1e9
            v["p99_duration"] = p99_duration / 1e9

        status_code = {i: v["status_code"].count(i) for i in set(v["status_code"])}
        request_content_length = {i: v["request_content_length"].count(i) for i in set(v["request_content_length"])}
        response_content_length = {i: v["response_content_length"].count(i) for i in set(v["response_content_length"])}

        # Calculate success rate
        total_requests = sum(status_code.values())
        success_count = status_code.get("200", 0)
        succ_rate = success_count / total_requests if total_requests > 0 else None

        v["status_code"] = status_code
        v["request_content_length"] = request_content_length
        v["response_content_length"] = response_content_length
        v["succ_rate"] = succ_rate

    return stat


def build_conclusion_row(
    k: str, v: dict[str, Any], normal_stat: dict[str, Any], abnormal_tag: dict[str, Any]
) -> ConclusionRow:
    return {
        "SpanName": k,
        "Issues": json.dumps(abnormal_tag),
        "AbnormalAvgDuration": v.get("avg_duration", 0.0),
        "NormalAvgDuration": normal_stat.get(k, {}).get("avg_duration", 0.0),
        "AbnormalSuccRate": v.get("succ_rate", 0.0),
        "NormalSuccRate": normal_stat.get(k, {}).get("succ_rate", 0.0),
        "AbnormalP90": v.get("p90_duration", 0.0),
        "NormalP90": normal_stat.get(k, {}).get("p90_duration", 0.0),
        "AbnormalP95": v.get("p95_duration", 0.0),
        "NormalP95": normal_stat.get(k, {}).get("p95_duration", 0.0),
        "AbnormalP99": v.get("p99_duration", 0.0),
        "NormalP99": normal_stat.get(k, {}).get("p99_duration", 0.0),
    }


def setup_paths_and_validation(in_p: Path | None, ou_p: Path | None) -> tuple[Path, Path, Path, Path]:
    """Setup and validate input/output paths and trace files."""
    if in_p is None:
        in_p = Path(os.environ.get("INPUT_PATH", ""))
    if ou_p is None:
        ou_p = Path(os.environ.get("OUTPUT_PATH", ""))

    input_path = Path(in_p)
    assert input_path.exists(), f"Input path does not exist: {input_path}"

    output_path = Path(ou_p)
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logger.info(f"Created output directory: {output_path}")

    normal_trace = Path(input_path) / "normal_traces.parquet"
    abnormal_trace = Path(input_path) / "abnormal_traces.parquet"
    assert normal_trace.exists(), f"Normal trace file does not exist: {normal_trace}"
    assert abnormal_trace.exists(), f"Abnormal trace file does not exist: {abnormal_trace}"

    return input_path, output_path, normal_trace, abnormal_trace


def initialize_analysis_state() -> AnalysisState:
    """Initialize the analysis state with default values."""
    return {
        "conclusion_data": [],
        "anomaly_count": 0,
        "processed_endpoints": 0,
        "skipped_endpoints": 0,
        "notations": {
            "issue_categories": {
                "latency_only": 0,
                "success_rate_only": 0,
                "both_latency_and_success_rate": 0,
                "no_issues": 0,
            },
            "total_endpoints": 0,
            "skipped_endpoints": 0,
            "absolute_anomaly": False,
            "anomaly_count": 0,
        },
    }


def get_percentile_config() -> list[tuple[str, str, Any]]:
    """Get the percentile configuration for anomaly detection."""
    return [
        ("avg", "avg_duration", lambda d: [x / 1e9 for x in d]),
        ("p90", "p90_duration", lambda d: sorted([x / 1e9 for x in d])),
        ("p95", "p95_duration", lambda d: sorted([x / 1e9 for x in d])),
        ("p99", "p99_duration", lambda d: sorted([x / 1e9 for x in d])),
    ]


def handle_new_endpoint(k: str, v: dict[str, Any], state: AnalysisState) -> None:
    """Handle endpoints that don't exist in normal data."""
    logger.warning(f"New endpoint found: {k} - checking against direct thresholds")
    state["skipped_endpoints"] += 1

    abnormal_tag = {}

    # Use direct thresholds from EnhancedLatencyConfig to detect anomalies
    # Hard timeout threshold (15.0s)
    hard_timeout_threshold = 15.0

    # Absolute anomaly thresholds for different percentiles
    absolute_thresholds = {
        "avg_duration": 3.0,
        "p90_duration": 7.0,
        "p95_duration": 8.0,
        "p99_duration": 10.0,
    }

    # Check latency thresholds
    for percentile_key, threshold in absolute_thresholds.items():
        if percentile_key in v:
            abnormal_value = v[percentile_key]
            if abnormal_value > threshold:
                abnormal_tag[percentile_key] = {
                    "normal": 0.0,  # No normal data available
                    "abnormal": abnormal_value,
                    "threshold": threshold,
                    "change_rate": float("inf"),  # Cannot calculate change rate without normal data
                    "absolute_change": abnormal_value,
                    "slo_violated": True,
                    "detection_reason": "new_endpoint_threshold_exceeded",
                }

    # Check hard timeout threshold
    avg_duration_val = v.get("avg_duration", 0.0)
    if avg_duration_val > hard_timeout_threshold:
        abnormal_tag["hard_timeout"] = {
            "threshold": hard_timeout_threshold,
            "abnormal": avg_duration_val,
            "slo_violated": True,
            "detection_reason": "hard_timeout_exceeded",
        }

    # Check success rate (assuming < 90% is anomalous for new endpoints)
    success_rate_threshold = 0.9
    total_requests = sum(v.get("status_code", {}).values())
    if total_requests > 0:
        success_count = v.get("status_code", {}).get("200", 0)
        success_rate = success_count / total_requests

        if success_rate < success_rate_threshold:
            abnormal_tag["succ_rate"] = {
                "normal": 1.0,  # Assume normal should be 100%
                "abnormal": success_rate,
                "threshold": success_rate_threshold,
                "rate_drop": 1.0 - success_rate,
                "slo_violated": True,
                "detection_reason": "new_endpoint_low_success_rate",
            }

    # Count anomalies and categorize issues
    if abnormal_tag:
        state["anomaly_count"] += 1

        # Categorize issues
        latency_keys = [
            "avg_duration",
            "p90_duration",
            "p95_duration",
            "p99_duration",
            "hard_timeout",
        ]
        has_latency_issue = any(key in abnormal_tag for key in latency_keys)
        has_success_rate_issue = "succ_rate" in abnormal_tag

        if has_latency_issue and has_success_rate_issue:
            state["notations"]["issue_categories"]["both_latency_and_success_rate"] += 1
        elif has_latency_issue:
            state["notations"]["issue_categories"]["latency_only"] += 1
        elif has_success_rate_issue:
            state["notations"]["issue_categories"]["success_rate_only"] += 1
    else:
        # No issues detected
        state["notations"]["issue_categories"]["no_issues"] += 1

    # Add to conclusion data
    state["conclusion_data"].append(build_conclusion_row(k, v, {}, abnormal_tag))


def detect_latency_anomalies(
    k: str,
    v: dict[str, Any],
    normal_stat: dict[str, Any],
    percentiles: list[tuple[str, str, Any]],
    state: AnalysisState,
) -> dict[str, Any]:
    """Detect latency anomalies for an endpoint."""
    abnormal_tag = {}
    normal_durations = [d / 1e9 for d in normal_stat[k]["duration"]]
    sorted_durations = sorted(normal_durations)

    for idx, (tp, key, norm_fn) in enumerate(percentiles):
        if tp == "avg":
            normal_data = norm_fn(normal_stat[k]["duration"])
            abnormal_value = v.get("avg_duration", 0.0)
        else:
            # p90, p95, p99
            if tp == "p90":
                start, end = int(len(sorted_durations) * 0.85), int(len(sorted_durations) * 0.95)
            elif tp == "p95":
                start, end = int(len(sorted_durations) * 0.90), int(len(sorted_durations) * 0.99)
            else:  # p99
                start, end = int(len(sorted_durations) * 0.95), len(sorted_durations)
            normal_data = sorted_durations[start:end] if start < end else sorted_durations
            abnormal_value = v.get(key, 0.0)

        if normal_data:
            from typing import cast

            result = calculate_anomaly_score(
                cast(Literal["avg", "p90", "p95", "p99"], tp),
                normal_data,
                abnormal_value,
            )
            if result.get("is_anomaly"):
                abnormal_tag[key] = {
                    "normal": normal_stat[k][key],
                    "abnormal": v[key],
                    "anomaly_score": result.get("total_score"),
                    "change_rate": result.get("change_rate"),
                    "absolute_change": result.get("abnormal_value"),
                    "slo_violated": True,
                }
                # Check if it's a rule-based anomaly
                if result.get("rule_anomaly"):
                    state["notations"]["absolute_anomaly"] = True

    return abnormal_tag


def detect_success_rate_anomalies(
    k: str,
    v: dict[str, Any],
    normal_stat: dict[str, Any],
    abnormal_tag: dict[str, Any],
    state: AnalysisState,
) -> dict[str, Any]:
    """Detect success rate anomalies for an endpoint."""
    normal_total = sum(normal_stat[k]["status_code"].values())
    abnormal_total = sum(v["status_code"].values())
    normal_succ_rate = normal_stat[k]["status_code"].get("200", 0) / max(normal_total, 1)
    abnormal_succ_rate = v["status_code"].get("200", 0) / max(abnormal_total, 1)

    success_rate_result = is_success_rate_significant(
        normal_succ_rate, abnormal_succ_rate, normal_total, abnormal_total
    )

    if success_rate_result.get("is_significant"):
        abnormal_tag["succ_rate"] = {
            "normal": normal_succ_rate,
            "abnormal": abnormal_succ_rate,
            "p_value": success_rate_result.get("p_value"),
            "z_statistic": success_rate_result.get("z_statistic"),
            "change_rate": success_rate_result.get("change_rate"),
            "rate_drop": success_rate_result.get("rate_drop"),
            "slo_violated": True,
        }
        logger.debug(
            f"Success rate anomaly detected for {k}: "
            f"drop={success_rate_result.get('rate_drop', 0.0):.3f}, "
            f"p_value={success_rate_result.get('p_value', 0.0):.3f}"
        )
        state["notations"]["absolute_anomaly"] = True

    return abnormal_tag


def categorize_issues(
    abnormal_tag: dict[str, Any],
    percentiles: list[tuple[str, str, Any]],
    state: AnalysisState,
) -> None:
    """Categorize the types of issues found."""
    latency_keys = [x[1] for x in percentiles]
    has_latency_issue = any(key in abnormal_tag for key in latency_keys)
    has_success_rate_issue = "succ_rate" in abnormal_tag

    if has_latency_issue and has_success_rate_issue:
        state["notations"]["issue_categories"]["both_latency_and_success_rate"] += 1
    elif has_latency_issue:
        state["notations"]["issue_categories"]["latency_only"] += 1
    elif has_success_rate_issue:
        state["notations"]["issue_categories"]["success_rate_only"] += 1
    else:
        state["notations"]["issue_categories"]["no_issues"] += 1


def analyze_single_endpoint(
    k: str,
    v: dict[str, Any],
    normal_stat: dict[str, Any],
    percentiles: list[tuple[str, str, Any]],
    state: AnalysisState,
) -> None:
    """Analyze a single endpoint for anomalies."""
    state["processed_endpoints"] += 1

    if k not in normal_stat:
        handle_new_endpoint(k, v, state)
        return

    # Detect latency anomalies
    abnormal_tag = detect_latency_anomalies(k, v, normal_stat, percentiles, state)

    # Detect success rate anomalies
    abnormal_tag = detect_success_rate_anomalies(k, v, normal_stat, abnormal_tag, state)

    # Count anomalies
    if abnormal_tag:
        state["anomaly_count"] += 1

    # Categorize issues
    categorize_issues(abnormal_tag, percentiles, state)

    # Add to conclusion data
    state["conclusion_data"].append(build_conclusion_row(k, v, normal_stat, abnormal_tag))


def validate_results(
    state: AnalysisState,
    normal_stat: dict[str, Any],
    abnormal_stat: dict[str, Any],
    input_path: Path,
) -> None:
    """Validate that we have conclusion data and log debug information if not."""
    if not state["conclusion_data"]:
        logger.error("No conclusion data generated!")
        logger.error(f"Normal stat keys: {list(normal_stat.keys())[:10]}...")
        logger.error(f"Abnormal stat keys: {list(abnormal_stat.keys())[:10]}...")
        logger.error(f"Normal stat count: {len(normal_stat)}")
        logger.error(f"Abnormal stat count: {len(abnormal_stat)}")

        normal_keys = set(normal_stat.keys())
        abnormal_keys = set(abnormal_stat.keys())
        common_keys = normal_keys.intersection(abnormal_keys)
        logger.error(f"Common keys count: {len(common_keys)}")
        if common_keys:
            logger.error(f"Sample common keys: {list(common_keys)[:5]}")

    assert state["conclusion_data"], f"No conclusion data generated! {input_path}, data: {state['conclusion_data']}"


def save_analysis_results(state: AnalysisState, output_path: Path) -> AnalysisState:
    """Save the analysis results to files."""
    # Save conclusion CSV
    conclusion = pl.DataFrame(state["conclusion_data"])
    conclusion.write_csv(Path(output_path) / "conclusion.csv")
    logger.info(f"Results saved to {Path(output_path) / 'conclusion.csv'}")

    # Update final notations
    state["notations"]["total_endpoints"] = state["processed_endpoints"]
    state["notations"]["skipped_endpoints"] = state["skipped_endpoints"]
    state["notations"]["anomaly_count"] = state["anomaly_count"]

    # Log summary
    logger.info("Issue category summary:")
    for category, count in state["notations"]["issue_categories"].items():
        count_int = int(count) if isinstance(count, int) else 0
        percentage = (count_int / state["processed_endpoints"] * 100) if state["processed_endpoints"] > 0 else 0
        logger.info(f"  {category}: {count} ({percentage:.1f}%)")

    # Save notations JSON
    with open(Path(output_path) / "notations.json", "w") as f:
        json.dump(state["notations"], f, indent=4)

    return state


def is_latency_only_dataset(state: AnalysisState) -> bool:
    issue_categories = state["notations"]["issue_categories"]
    return (
        issue_categories["latency_only"] > 0
        and issue_categories["success_rate_only"] == 0
        and issue_categories["both_latency_and_success_rate"] == 0
    )


@app.command()
@timeit()
def run(in_p: Path | None = None, ou_p: Path | None = None, convert: bool = True) -> AnalysisResult:
    """Run RCA analysis and return metadata about the analysis."""
    input_path, output_path, normal_trace, abnormal_trace = setup_paths_and_validation(in_p, ou_p)

    normal_stat = preprocess_trace(normal_trace)
    abnormal_stat = preprocess_trace(abnormal_trace)

    assert normal_stat, f"No endpoints found in normal trace data: {normal_trace}"
    assert abnormal_stat, f"No endpoints found in abnormal trace data: {abnormal_trace}"

    # Initialize analysis state and configuration
    state = initialize_analysis_state()
    percentiles = get_percentile_config()

    # Process each endpoint
    for k, v in abnormal_stat.items():
        analyze_single_endpoint(k, v, normal_stat, percentiles, state)

    # Validate and save results
    validate_results(state, normal_stat, abnormal_stat, input_path)
    state = save_analysis_results(state, output_path)

    # Check if this is a latency-only dataset
    is_latency_only = is_latency_only_dataset(state)

    # Get datapack name from input path
    datapack_name = input_path.name

    if convert:
        platform_convert(in_p, ou_p)

    # Return analysis metadata
    result: AnalysisResult = {
        "datapack_name": datapack_name,
        "is_latency_only": is_latency_only,
        "total_endpoints": state["processed_endpoints"],
        "anomaly_count": state["anomaly_count"],
        "issue_categories": state["notations"]["issue_categories"],
        "absolute_anomaly": state["notations"]["absolute_anomaly"],
    }
    return result


def platform_convert(in_p: Path | None = None, ou_p: Path | None = None):
    from rcabench_platform.v2.sources.convert import convert_datapack
    from rcabench_platform.v2.sources.rcabench import RcabenchDatapackLoader

    if in_p is None:
        in_p = Path(os.environ.get("INPUT_PATH", ""))
    if ou_p is None:
        ou_p = Path(os.environ.get("OUTPUT_PATH", ""))

    input_path = in_p
    output_path = ou_p
    assert input_path.exists(), f"Input path does not exist: {input_path}"
    assert output_path.exists(), f"Output path does not exist: {output_path}"

    # Assert injection.json exists and is valid
    injection_file = input_path / "injection.json"
    assert injection_file.exists(), f"injection.json not found in {input_path}"

    with open(injection_file) as f:
        injection = json.load(f)
        injection_name = injection.get("injection_name")
        assert injection_name and isinstance(injection_name, str), (
            f"Invalid injection_name in {injection_file}: {injection_name}"
        )

    # Assert essential trace files exist and are not empty
    normal_traces = input_path / "normal_traces.parquet"
    abnormal_traces = input_path / "abnormal_traces.parquet"

    assert normal_traces.exists(), f"normal_traces.parquet not found in {input_path}"
    assert abnormal_traces.exists(), f"abnormal_traces.parquet not found in {input_path}"

    # Assert trace files are not empty
    normal_df = pl.scan_parquet(normal_traces)
    normal_count = normal_df.select(pl.len()).collect().item()
    assert normal_count > 0, f"normal_traces.parquet is empty in {input_path}"

    abnormal_df = pl.scan_parquet(abnormal_traces)
    abnormal_count = abnormal_df.select(pl.len()).collect().item()
    assert abnormal_count > 0, f"abnormal_traces.parquet is empty in {input_path}"

    logger.info(f"Trace files validated: normal={normal_count} records, abnormal={abnormal_count} records")

    converted_input_path = output_path / "converted"

    convert_datapack(
        loader=RcabenchDatapackLoader(src_folder=input_path, datapack=injection_name),
        dst_folder=converted_input_path,
        skip_finished=True,
    )
    logger.info(f"Successfully converted datapack for {injection_name}")


def datapack_validation(datapack: Path) -> bool:
    """Validate that a datapack directory contains all required files and they are not empty.

    Returns:
        bool: True if validation passes, False otherwise.
    """
    required_files = [
        "abnormal_logs.parquet",
        "abnormal_metrics_histogram.parquet",
        "abnormal_metrics.parquet",
        "abnormal_metrics_sum.parquet",
        "abnormal_trace_id_ts.parquet",
        "abnormal_traces.parquet",
        "env.json",
        "injection.json",
        "k8s.json",
        "normal_logs.parquet",
        "normal_metrics_histogram.parquet",
        "normal_metrics.parquet",
        "normal_metrics_sum.parquet",
        "normal_trace_id_ts.parquet",
        "normal_traces.parquet",
    ]

    # Check if datapack directory exists
    if not datapack.exists():
        logger.error(f"Datapack directory does not exist: {datapack}")
        return False

    if not datapack.is_dir():
        logger.error(f"Datapack path is not a directory: {datapack}")
        return False

    missing_files = []
    empty_files = []

    for filename in required_files:
        file_path = datapack / filename

        # Check if file exists
        if not file_path.exists():
            missing_files.append(filename)
            continue

        # Check if file is empty
        try:
            if filename.endswith(".parquet"):
                # For parquet files, check if they contain data
                df = pl.scan_parquet(file_path)
                count = df.select(pl.len()).collect().item()
                if count == 0:
                    empty_files.append(filename)
            elif filename.endswith(".json"):
                # For JSON files, check if they have content
                file_size = file_path.stat().st_size
                if file_size == 0:
                    empty_files.append(filename)
                else:
                    # Also validate that JSON is parseable
                    with open(file_path) as f:
                        try:
                            json.load(f)
                        except json.JSONDecodeError:
                            empty_files.append(f"{filename} (invalid JSON)")
        except Exception as e:
            logger.error(f"Error validating file {filename}: {e}")
            empty_files.append(f"{filename} (validation error)")

    # Log errors if validation fails
    if missing_files:
        logger.error(f"Missing files in {datapack}: {', '.join(missing_files)}")

    if empty_files:
        logger.error(f"Empty or invalid files in {datapack}: {', '.join(empty_files)}")

    if missing_files or empty_files:
        return False

    return True


@app.command()
@timeit()
def validate_datapacks(delete_invalid: bool = False) -> dict[str, Any]:
    dataset_path = Path("data") / "rcabench_dataset"
    assert dataset_path.exists(), f"Dataset path does not exist: {dataset_path}"

    # Get all datapack directories
    datapack_paths = [p for p in dataset_path.iterdir() if p.is_dir()]
    total_datapacks = len(datapack_paths)

    if total_datapacks == 0:
        logger.warning(f"No datapack directories found in {dataset_path}")
        return {"total": 0, "valid": 0, "invalid": 0, "deleted": 0}

    logger.info(f"Found {total_datapacks} datapacks to validate")

    cpu = os.cpu_count()
    assert cpu is not None, "Cannot determine CPU count"
    parallel = max(1, cpu // 4)

    validation_tasks = [functools.partial(datapack_validation, dp) for dp in datapack_paths]

    # Run validation in parallel
    validation_results = fmap_processpool(validation_tasks, parallel=parallel, cpu_limit_each=1, ignore_exceptions=True)

    # Process results
    valid_datapacks = []
    invalid_datapacks: list[Path] = []

    for i, (datapack_path, is_valid) in enumerate(zip(datapack_paths, validation_results)):
        if is_valid:
            valid_datapacks.append(datapack_path)
        else:
            invalid_datapacks.append(datapack_path)

    # Summary statistics
    valid_count = len(valid_datapacks)
    invalid_count = len(invalid_datapacks)
    deleted_count = 0

    logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid")

    if delete_invalid and invalid_datapacks:
        logger.warning(f"Deleting {len(invalid_datapacks)} invalid datapacks...")

        for datapack in invalid_datapacks:
            try:
                import shutil

                for file_path in datapack.iterdir():
                    if file_path.is_file():
                        file_path.unlink()
                    elif file_path.is_dir():
                        shutil.rmtree(file_path)

                datapack.rmdir()
                deleted_count += 1

            except Exception as e:
                logger.error(f"Failed to delete {datapack.name}: {e}")

    summary = {
        "total": total_datapacks,
        "valid": valid_count,
        "invalid": invalid_count,
        "deleted": deleted_count,
    }

    logger.info(f"Final summary: {summary}")
    return summary


@app.command()
@timeit()
def local_test(datapack: str):
    input_path = Path("data") / "rcabench_dataset" / datapack
    output_path = Path("temp") / "detector" / datapack
    output_path.mkdir(parents=True, exist_ok=True)

    os.environ["INPUT_PATH"] = str(input_path)
    os.environ["OUTPUT_PATH"] = str(output_path)

    result = run(convert=False)

    logger.info(f"Analysis Results for {datapack}:")
    logger.info(f"  - Is Latency Only: {result['is_latency_only']}")
    logger.info(f"  - Total Endpoints: {result['total_endpoints']}")
    logger.info(f"  - Anomaly Count: {result['anomaly_count']}")
    logger.info(f"  - Issue Categories: {result['issue_categories']}")

    return result


@app.command()
@timeit()
def patch_detection():
    """Run patch detection on all valid datapacks.

    Args:
        dataset_path: Path to the dataset directory
        delete_invalid: Whether to delete invalid datapacks during validation
        validate_first: Whether to run validation before processing
        parallel: Number of parallel processes
    """
    dataset_path: Path = Path("data") / "rcabench_dataset"
    assert dataset_path.exists(), f"Dataset path does not exist: {dataset_path}"

    tasks = []
    assertions = []

    for datapack in dataset_path.iterdir():
        try:
            if not datapack.is_dir():
                continue

            trace_files = [
                datapack / "abnormal_traces.parquet",
                datapack / "normal_traces.parquet",
            ]
            missing_files = [f.name for f in trace_files if not f.exists()]
            assert all(f.exists() for f in trace_files), f"Missing trace files in {datapack}: {missing_files}"

            # Assert injection.json exists
            injection_file = datapack / "injection.json"
            assert injection_file.exists(), f"Missing injection.json in {str(datapack)}"

            # Validate injection.json content
            with open(injection_file) as f:
                injection = json.load(f)
                injection_name = injection.get("injection_name")
                assert injection_name and isinstance(injection_name, str), (
                    f"Invalid injection_name in {datapack.name}/injection.json: {injection_name}"
                )

            for trace_file in trace_files:
                df = pl.scan_parquet(trace_file)
                count = df.select(pl.len()).collect().item()
                assert count > 0, f"Empty trace file: {trace_file}"

            tasks.append(functools.partial(run, in_p=datapack, ou_p=datapack, convert=False))
        except Exception as e:
            assertions.append((datapack.name, str(e)))

    logger.info(f"Found {len(tasks)} valid datapacks to process")
    assert len(tasks) > 0, "No valid datapacks found to process"

    cpu = os.cpu_count()
    assert cpu is not None, "Cannot determine CPU count"

    parallel = cpu // 4
    results = fmap_processpool(tasks, parallel=parallel, cpu_limit_each=4, ignore_exceptions=True)

    # Create temp directory if it doesn't exist
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)

    with open("temp/patch_assertions.txt", "w") as f:
        for datapack_name, error in assertions:
            f.write(f"{datapack_name}: {error}\n")
            logger.error(f"Assertion failed for {datapack_name}: {error}")

    with open("temp/patch_results.txt", "w") as f:
        for result in results:
            if result["is_latency_only"] and not result["absolute_anomaly"]:
                f.write(f"{result['datapack_name']}\n")


if __name__ == "__main__":
    app()
