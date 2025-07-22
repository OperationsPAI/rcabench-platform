#!/usr/bin/env -S uv run -s
"""
Migrated from https://github.com/LGU-SE-Internal/ts-anomaly-detector
"""

from zoneinfo import ZoneInfo
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.utils.fmap import fmap_processpool, fmap_threadpool
from rcabench_platform.v2.datasets.train_ticket import extract_path
from pathlib import Path
import json
import re
import os
import polars as pl
import scipy.stats as stats
import numpy as np
import functools
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone
from typing import Literal


def calculate_anomaly_score(tp: Literal["avg", "p90", "p95", "p99"], normal_data: list, abnormal_value: float) -> dict:
    # Require at least 5 data points for reliable statistical analysis
    if len(normal_data) < 5:
        return {"total_score": 0, "is_anomaly": False, "change_rate": 0}

    normal_array = np.array(normal_data)
    normal_mean = np.mean(normal_array)
    normal_std = np.std(normal_array)

    # Only consider it anomalous if abnormal_value is HIGHER than normal (worse performance)
    if abnormal_value <= normal_mean:
        return {"total_score": 0, "is_anomaly": False, "change_rate": 0}

    # Calculate Z-score to measure how many standard deviations the value deviates from normal
    # Use signed difference since we only care about increases
    z_score = (abnormal_value - normal_mean) / normal_std if normal_std > 0 else 0

    # Calculate relative change as percentage increase from normal mean
    relative_change = (abnormal_value - normal_mean) / normal_mean if normal_mean > 0 else 0

    # Calculate absolute change in original units (always positive since abnormal_value > normal_mean)
    absolute_change = abnormal_value - normal_mean

    # Calculate coefficient of variation (CV) for normal data stability assessment
    # Lower CV means more stable normal data, which should amplify anomaly scores
    cv = normal_std / normal_mean if normal_mean > 0 else float("inf")

    # Type-specific tolerance factors: higher percentiles should be more tolerant
    # avg: baseline tolerance, p90: +20% tolerance, p95: +40% tolerance, p99: +60% tolerance
    type_tolerance_factors = {
        "avg": 1.0,  # baseline
        "p90": 1.2,  # 20% more tolerant
        "p95": 1.4,  # 40% more tolerant
        "p99": 1.6,  # 60% more tolerant
    }
    type_tolerance = type_tolerance_factors.get(tp, 1.0)

    # Calculate stability factor: more stable normal data (lower CV) gets higher factor
    # CV < 0.1 (very stable) -> factor ~1.5, CV > 0.5 (unstable) -> factor ~0.5
    # Adjusted for services that are inherently unstable and type-specific tolerance
    stability_factor = max(0.3, min(1.6, float(1.0 / (1.0 + cv * 1.5 * type_tolerance))))

    # Calculate baseline quality factor: lower normal mean indicates better baseline performance
    # Mean < 0.5s -> factor ~1.3, Mean > 2.0s -> factor ~0.7
    # Adjusted to be more lenient for higher baseline latencies and type-specific tolerance
    baseline_quality_factor = max(0.5, min(1.4, float(1.0 / (1.0 + normal_mean * 0.3 * type_tolerance))))

    # Combined quality multiplier: stable + low baseline = higher scores
    quality_multiplier = (stability_factor + baseline_quality_factor) / 2.0

    # Normalize Z-score to [0,1] range, treating 3 standard deviations as maximum
    # Since we already ensured abnormal_value > normal_mean, z_score should be positive
    z_score_norm = min(float(z_score / 3), 1.0) if z_score > 0 else 0.0

    # Normalize relative change to [0,1] range, treating 100% change as maximum
    relative_change_norm = min(float(relative_change / 1.0), 1.0)
    # Normalize absolute change to [0,1] range, treating 0.1 units as maximum
    absolute_change_norm = min(float(absolute_change / 0.1), 1.0)

    # Calculate severity multiplier based on absolute values
    # Higher absolute values (>1s) get amplified relative change weight
    # Lower absolute values (<0.1s) get reduced relative change weight
    severity_factor = min(max(abnormal_value / 1.0, 0.3), 3.0)  # Range: [0.3, 3.0]

    # Dynamic weight calculation based on severity
    # Base weights: z_score=30%, relative=40%, absolute=30%
    # Adjust relative weight based on severity factor
    relative_weight = min(0.4 * severity_factor, 0.7)  # Max 70% for high severity
    remaining_weight = 1.0 - relative_weight
    z_score_weight = remaining_weight * 0.5  # Split remaining between z_score and absolute
    absolute_weight = remaining_weight * 0.5

    # Calculate base score without quality adjustment
    base_score = (
        z_score_norm * z_score_weight + relative_change_norm * relative_weight + absolute_change_norm * absolute_weight
    )

    # Apply quality multiplier to reward stable, low-baseline normal data
    total_score = min(1.0, base_score * quality_multiplier)

    # Adjust anomaly threshold based on data quality and type-specific tolerance
    # Higher quality normal data (stable + low baseline) allows lower threshold
    # More aggressive adjustment for unstable services
    # Higher percentiles get higher thresholds (more tolerant)
    base_threshold = 0.6
    type_threshold_adjustments = {
        "avg": 0.0,  # baseline threshold
        "p90": 0.05,  # +0.05 higher threshold (more tolerant)
        "p95": 0.08,  # +0.08 higher threshold
        "p99": 0.12,  # +0.12 higher threshold (most tolerant)
    }
    type_threshold_adj = type_threshold_adjustments.get(tp, 0.0)

    dynamic_threshold = max(0.35, base_threshold + type_threshold_adj - (quality_multiplier - 1.0) * 0.4)

    # Type-specific detection conditions: higher percentiles need larger changes
    type_relative_thresholds = {
        "avg": 0.3,  # 30% for average
        "p90": 0.4,  # 40% for p90
        "p95": 0.5,  # 50% for p95
        "p99": 0.6,  # 60% for p99
    }
    type_absolute_thresholds = {
        "avg": 0.5,  # 0.5s for average
        "p90": 0.8,  # 0.8s for p90
        "p95": 1.0,  # 1.0s for p95
        "p99": 1.5,  # 1.5s for p99
    }

    relative_threshold = type_relative_thresholds.get(tp, 0.3)
    absolute_threshold = type_absolute_thresholds.get(tp, 0.5)

    is_anomaly = (
        total_score > dynamic_threshold
        and relative_change > relative_threshold
        and absolute_change > absolute_threshold
    )

    ret = {
        "total_score": total_score,
        "is_anomaly": is_anomaly,
        "change_rate": relative_change,
        "absolute_change": absolute_change,
        "z_score": z_score,
        "normal_mean": normal_mean,
        "normal_std": normal_std,
        "abnormal_value": abnormal_value,
        "severity_factor": severity_factor,
        "cv": cv,
        "type_tolerance": type_tolerance,
        "stability_factor": stability_factor,
        "baseline_quality_factor": baseline_quality_factor,
        "quality_multiplier": quality_multiplier,
        "base_score": base_score,
        "dynamic_threshold": dynamic_threshold,
        "relative_threshold": relative_threshold,
        "absolute_threshold": absolute_threshold,
        "weights": {
            "z_score": z_score_weight,
            "relative_change": relative_weight,
            "absolute_change": absolute_weight,
        },
    }

    if abnormal_value > 15.0:
        # hardcoded rule for long duration: the client timeout range is 20s
        ret["is_anomaly"] = True
        ret["rule_anomaly"] = "hardcoded_long_duration"
        return ret
    if tp == "avg" and normal_mean > 2.0:
        # if normal mean is already high, we consider the data is not stable
        # TODO: record the normal mean and percentiles for further dataset difficulty analysis
        logger.warning(f"normal duration too large: {tp}[{normal_mean}]")
        ret["is_anomaly"] = False

    if tp == "p90" and normal_mean > 3.0:
        ret["is_anomaly"] = False
        logger.warning(f"normal duration too large: {tp}[{normal_mean}]")

    if tp == "p95" and normal_mean > 5.0:
        ret["is_anomaly"] = False
        logger.warning(f"normal duration too large: {tp}[{normal_mean}]")

    if tp == "p99" and normal_mean > 6.0:
        ret["is_anomaly"] = False
        logger.warning(f"normal duration too large: {tp}[{normal_mean}]")

    return ret


def is_success_rate_significant(
    normal_rate: float, abnormal_rate: float, normal_count: int, abnormal_count: int
) -> dict:
    if normal_count < 10 or abnormal_count < 5:
        return {"is_significant": False, "reason": "insufficient_data", "change_rate": 0}

    p1, n1 = normal_rate, normal_count
    p2, n2 = abnormal_rate, abnormal_count

    pooled_p = (p1 * n1 + p2 * n2) / (n1 + n2)
    se = np.sqrt(pooled_p * (1 - pooled_p) * (1 / n1 + 1 / n2))

    z_stat = abs(p2 - p1) / se if se > 0 else 0
    p_value = 2 * (1 - stats.norm.cdf(abs(z_stat))) if se > 0 else 1.0

    rate_drop = normal_rate - abnormal_rate

    is_significant = rate_drop > 0.03 and p_value < 0.05 and rate_drop > 0.1 * normal_rate

    return {
        "is_significant": is_significant,
        "p_value": p_value,
        "z_statistic": z_stat,
        "rate_drop": rate_drop,
        "change_rate": rate_drop,
        "normal_rate": normal_rate,
        "abnormal_rate": abnormal_rate,
    }


def read_dataframe(file: Path) -> pl.LazyFrame:
    return pl.scan_parquet(file)


def preprocess_trace(file: Path):
    if not file.exists():
        logger.error(f"Trace file does not exist: {file}")
        return {}

    df = read_dataframe(file)

    entry_df = df.filter(
        (pl.col("ServiceName") == "loadgenerator-service")
        & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
    )

    entry_count = entry_df.select(pl.len()).collect().item()
    if entry_count == 0:
        logger.error("loadgenerator-service not found in trace data, using ts-ui-dashboard as fallback")
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

    logger.info(f"Loaded trace file with {len(entry_df_collected)} records")
    entrypoints = set(entry_df_collected["SpanName"].to_list())
    logger.info(f"Found {len(entrypoints)} unique endpoints")

    deduped_entrypoints = {}
    for entrypoint in entrypoints:
        path = extract_path(entrypoint)
        deduped_entrypoints[entrypoint] = path
    logger.info(f"Deduplication complete, found {len(set(deduped_entrypoints.values()))} unique paths")

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
            continue

        durations_array = np.array(durations)
        avg_duration = np.mean(durations_array)
        p90_duration = np.percentile(durations_array, 90)
        p95_duration = np.percentile(durations_array, 95)
        p99_duration = np.percentile(durations_array, 99)

        status_code = {i: v["status_code"].count(i) for i in set(v["status_code"])}
        request_content_length = {i: v["request_content_length"].count(i) for i in set(v["request_content_length"])}
        response_content_length = {i: v["response_content_length"].count(i) for i in set(v["response_content_length"])}

        v["avg_duration"] = avg_duration / 1e9
        v["p90_duration"] = p90_duration / 1e9
        v["p95_duration"] = p95_duration / 1e9
        v["p99_duration"] = p99_duration / 1e9
        v["status_code"] = status_code
        v["request_content_length"] = request_content_length
        v["response_content_length"] = response_content_length

    logger.info(f"Successfully processed {len(stat)} endpoints from trace file")
    return stat


def build_conclusion_row(k, v, normal_stat, abnormal_tag):
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


@app.command()
@timeit()
def run(in_p: Path | None = None, ou_p: Path | None = None, convert: bool = True):
    logger.info("Starting RCA analysis")
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
    logger.info(f"Processing normal trace file: {normal_trace}")
    normal_stat = preprocess_trace(normal_trace)

    logger.info(f"Normal trace processing result: {len(normal_stat)} endpoints found")
    logger.info(f"Processing abnormal trace file: {abnormal_trace}")

    abnormal_stat = preprocess_trace(abnormal_trace)

    logger.info(f"Abnormal trace processing result: {len(abnormal_stat)} endpoints found")

    assert normal_stat, f"No endpoints found in normal trace data: {normal_trace}"
    assert abnormal_stat, f"No endpoints found in abnormal trace data: {abnormal_trace}"
    conclusion_data = []
    anomaly_count = 0
    processed_endpoints = 0
    skipped_endpoints = 0
    notations = {
        "issue_categories": {
            "latency_only": 0,
            "success_rate_only": 0,
            "both_latency_and_success_rate": 0,
            "no_issues": 0,
        },
        "total_endpoints": 0,
        "skipped_endpoints": 0,
        "absolute_anomaly": False,
    }
    percentiles = [
        ("avg", "avg_duration", lambda d: [x / 1e9 for x in d]),
        ("p90", "p90_duration", lambda d: sorted([x / 1e9 for x in d])),
        ("p95", "p95_duration", lambda d: sorted([x / 1e9 for x in d])),
        ("p99", "p99_duration", lambda d: sorted([x / 1e9 for x in d])),
    ]
    for k, v in abnormal_stat.items():
        processed_endpoints += 1
        if k not in normal_stat:
            logger.warning(f"New endpoint found: {k} - skipping comparison")
            skipped_endpoints += 1
            new_endpoint_issues = {"new_endpoint": {"slo_violated": True, "reason": "endpoint_not_in_normal_data"}}
            notations["issue_categories"]["latency_only"] += 1
            conclusion_data.append(
                {
                    "SpanName": k,
                    "Issues": json.dumps(new_endpoint_issues),
                    "AbnormalAvgDuration": v["avg_duration"],
                    "NormalAvgDuration": 0.0,
                    "AbnormalSuccRate": 0.0,
                    "NormalSuccRate": 0.0,
                    "AbnormalP90": v["p90_duration"],
                    "NormalP90": 0.0,
                    "AbnormalP95": v["p95_duration"],
                    "NormalP95": 0.0,
                    "AbnormalP99": v["p99_duration"],
                    "NormalP99": 0.0,
                }
            )
            continue
        abnormal_tag = {}
        normal_durations = [d / 1e9 for d in normal_stat[k]["duration"]]
        sorted_durations = sorted(normal_durations)
        for idx, (tp, key, norm_fn) in enumerate(percentiles):
            if tp == "avg":
                normal_data = norm_fn(normal_stat[k]["duration"])
                abnormal_value = v["avg_duration"]
            else:
                # p90, p95, p99
                if tp == "p90":
                    start, end = int(len(sorted_durations) * 0.85), int(len(sorted_durations) * 0.95)
                elif tp == "p95":
                    start, end = int(len(sorted_durations) * 0.90), int(len(sorted_durations) * 0.99)
                else:  # p99
                    start, end = int(len(sorted_durations) * 0.95), len(sorted_durations)
                normal_data = sorted_durations[start:end] if start < end else sorted_durations
                abnormal_value = v[key]
            if normal_data:
                from typing import cast

                result = calculate_anomaly_score(
                    cast(Literal["avg", "p90", "p95", "p99"], tp), normal_data, abnormal_value
                )
                if result["is_anomaly"]:
                    abnormal_tag[key] = {
                        "normal": normal_stat[k][key],
                        "abnormal": v[key],
                        "anomaly_score": result["total_score"],
                        "change_rate": result["change_rate"],
                        "absolute_change": result["absolute_change"],
                        "slo_violated": True,
                    }
                    if "rule_anomaly" in result:
                        notations["absolute_anomaly"] = True
                    logger.debug(
                        f"{tp.upper()} duration anomaly detected for {k}: "
                        f"score={result['total_score']:.3f}, "
                        f"change={result['change_rate']:.1f}, "
                        f"abs_change={result['absolute_change']:.1f}"
                    )
        normal_total = sum(normal_stat[k]["status_code"].values())
        abnormal_total = sum(v["status_code"].values())
        normal_succ_rate = normal_stat[k]["status_code"].get("200", 0) / max(normal_total, 1)
        abnormal_succ_rate = v["status_code"].get("200", 0) / max(abnormal_total, 1)
        success_rate_result = is_success_rate_significant(
            normal_succ_rate, abnormal_succ_rate, normal_total, abnormal_total
        )
        if success_rate_result["is_significant"]:
            abnormal_tag["succ_rate"] = {
                "normal": normal_succ_rate,
                "abnormal": abnormal_succ_rate,
                "p_value": success_rate_result["p_value"],
                "z_statistic": success_rate_result["z_statistic"],
                "change_rate": success_rate_result["change_rate"],
                "rate_drop": success_rate_result["rate_drop"],
                "slo_violated": True,
            }
            logger.debug(
                f"Success rate anomaly detected for {k}: "
                f"drop={success_rate_result['rate_drop']:.3f}, "
                f"p_value={success_rate_result['p_value']:.3f}"
            )
            notations["absolute_anomaly"] = True
        if abnormal_tag:
            anomaly_count += 1
        latency_keys = [x[1] for x in percentiles]
        has_latency_issue = any(key in abnormal_tag for key in latency_keys)
        has_success_rate_issue = "succ_rate" in abnormal_tag
        if has_latency_issue and has_success_rate_issue:
            notations["issue_categories"]["both_latency_and_success_rate"] += 1
        elif has_latency_issue:
            notations["issue_categories"]["latency_only"] += 1
        elif has_success_rate_issue:
            notations["issue_categories"]["success_rate_only"] += 1
        else:
            notations["issue_categories"]["no_issues"] += 1
        conclusion_data.append(build_conclusion_row(k, v, normal_stat, abnormal_tag))
    logger.info(f"Analysis complete. Found {anomaly_count} endpoints with anomalies")
    logger.info(f"Processed {processed_endpoints} endpoints, skipped {skipped_endpoints} endpoints")
    logger.info(f"Total conclusion data entries: {len(conclusion_data)}")
    if not conclusion_data:
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
    assert conclusion_data, f"No conclusion data generated! {input_path}, data: {conclusion_data}"
    conclusion = pl.DataFrame(conclusion_data)
    conclusion.write_csv(Path(output_path) / "conclusion.csv")
    logger.info(f"Results saved to {Path(output_path) / 'conclusion.csv'}")
    notations["total_endpoints"] = processed_endpoints
    notations["skipped_endpoints"] = skipped_endpoints
    notations["anomaly_count"] = anomaly_count
    logger.info("Issue category summary:")
    for category, count in notations["issue_categories"].items():
        percentage = (count / processed_endpoints * 100) if processed_endpoints > 0 else 0
        logger.info(f"  {category}: {count} ({percentage:.1f}%)")
    with open(Path(output_path) / "notations.json", "w") as f:
        json.dump(notations, f, indent=4)
    if convert:
        platform_convert(in_p, ou_p)


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


@app.command()
@timeit()
def local_test(datapack: str):
    input_path = Path("data") / "rcabench_dataset" / datapack
    output_path = Path("temp") / "detector" / datapack
    output_path.mkdir(parents=True, exist_ok=True)

    os.environ["INPUT_PATH"] = str(input_path)
    os.environ["OUTPUT_PATH"] = str(output_path)

    run()


@app.command()
@timeit()
def patch_detection():
    input_path = Path("data") / "rcabench_dataset"
    assert input_path.exists(), f"Dataset path does not exist: {input_path}"

    tasks = []

    assertions = []

    for datapack in input_path.iterdir():
        try:
            if not datapack.is_dir():
                continue

            trace_files = [datapack / "abnormal_traces.parquet", datapack / "normal_traces.parquet"]
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

            # Validate trace files are not empty
            for trace_file in trace_files:
                df = pl.scan_parquet(trace_file)
                count = df.select(pl.len()).collect().item()
                assert count > 0, f"Empty trace file: {trace_file}"

            tasks.append(functools.partial(run, in_p=datapack, ou_p=datapack, convert=False))
        except AssertionError as e:
            assertions.append((datapack.name, str(e)))

    logger.info(f"Found {len(tasks)} valid datapacks to process")
    assert len(tasks) > 0, "No valid datapacks found to process"

    cpu = os.cpu_count()
    assert cpu is not None, "Cannot determine CPU count"

    # Remove ignore_exceptions=True to fail fast on any error
    fmap_processpool(tasks, parallel=cpu // 4, cpu_limit_each=4)

    with open("temp/patch_assertions.txt", "w") as f:
        for datapack_name, error in assertions:
            f.write(f"{datapack_name}: {error}\n")
            logger.error(f"Assertion failed for {datapack_name}: {error}")


if __name__ == "__main__":
    app()
