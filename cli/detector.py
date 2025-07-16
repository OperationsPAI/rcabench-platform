#!/usr/bin/env -S uv run -s
"""
Migrated from https://github.com/LGU-SE-Internal/ts-anomaly-detector
"""

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.utils.fmap import fmap_processpool, fmap_threadpool

from pathlib import Path
import json
import re
import os
import polars as pl
import scipy.stats as stats
import numpy as np
import functools


def calculate_anomaly_score(normal_data: list, abnormal_value: float) -> dict:
    # Require at least 5 data points for reliable statistical analysis
    if len(normal_data) < 5:
        return {"total_score": 0, "is_anomaly": False, "change_rate": 0}

    normal_array = np.array(normal_data)
    normal_mean = np.mean(normal_array)
    normal_std = np.std(normal_array)

    # Calculate Z-score to measure how many standard deviations the value deviates from normal
    z_score = abs(abnormal_value - normal_mean) / normal_std if normal_std > 0 else 0

    # Calculate relative change as percentage deviation from normal mean
    relative_change = abs(abnormal_value - normal_mean) / normal_mean if normal_mean > 0 else 0

    # Calculate absolute change in original units
    absolute_change = abs(abnormal_value - normal_mean)

    # Normalize Z-score to [0,1] range, treating 3 standard deviations as maximum
    z_score_norm = min(float(z_score / 3), 1.0)

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

    total_score = (
        z_score_norm * z_score_weight + relative_change_norm * relative_weight + absolute_change_norm * absolute_weight
    )

    is_anomaly = total_score > 0.6 and relative_change > 0.3 and absolute_change > 2

    return {
        "total_score": total_score,
        "is_anomaly": is_anomaly,
        "change_rate": relative_change,
        "absolute_change": absolute_change,
        "z_score": z_score,
        "normal_mean": normal_mean,
        "abnormal_value": abnormal_value,
        "severity_factor": severity_factor,
        "weights": {
            "z_score": z_score_weight,
            "relative_change": relative_weight,
            "absolute_change": absolute_weight,
        },
    }


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


def extract_path(uri: str):
    from rcabench_platform.v2.datasets.train_ticket import PATTERN_REPLACEMENTS

    for pattern, replacement in PATTERN_REPLACEMENTS:
        res = re.sub(pattern, replacement, uri)
        if res != uri:
            return res
    return uri


def read_dataframe(file: Path) -> pl.LazyFrame:
    return pl.scan_parquet(file)


def preprocess_trace(file: Path):
    df = read_dataframe(file)

    entry_df = df.filter(pl.col("ServiceName") == "loadgenerator-service")

    entry_count = entry_df.select(pl.count()).collect().item()
    if entry_count == 0:
        logger.error("loadgenerator-service not found in trace data, using ts-ui-dashboard as fallback")
        entry_df = df.filter(pl.col("ServiceName") == "ts-ui-dashboard")
        entry_count = entry_df.select(pl.count()).collect().item()

    if entry_count == 0:
        logger.error("No valid entrypoint found in trace data, aborting")
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

    return stat


@app.command()
@timeit()
def run(in_p: Path | None = None, ou_p: Path | None = None, convert: bool = True):
    logger.info("Starting RCA analysis")

    if in_p is None:
        in_p = Path(os.environ.get("INPUT_PATH", ""))
    if ou_p is None:
        ou_p = Path(os.environ.get("OUTPUT_PATH", ""))

    input_path = Path(in_p)
    assert input_path.exists()

    output_path = Path(ou_p)
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logger.info(f"Created output directory: {output_path}")

    normal_trace = Path(input_path) / "normal_traces.parquet"
    abnormal_trace = Path(input_path) / "abnormal_traces.parquet"

    logger.info(f"Processing normal trace file: {normal_trace}")
    normal_stat = preprocess_trace(normal_trace)
    logger.info(f"Processing abnormal trace file: {abnormal_trace}")
    abnormal_stat = preprocess_trace(abnormal_trace)

    conclusion_data = []

    anomaly_count = 0
    for k, v in abnormal_stat.items():
        if k not in normal_stat:
            logger.warning(f"New endpoint found: {k} - skipping comparison")
            continue

        logger.debug(f"Analyzing endpoint: {k}")
        abnormal_tag = {}

        avg_duration_result = {"total_score": 0, "is_anomaly": False, "change_rate": 0}
        success_rate_result = {"is_significant": False, "p_value": 1.0}

        normal_durations = [d / 1e9 for d in normal_stat[k]["duration"]]

        avg_duration_result = calculate_anomaly_score(normal_durations, v["avg_duration"])
        if avg_duration_result["is_anomaly"]:
            abnormal_tag["avg_duration"] = {
                "normal": normal_stat[k]["avg_duration"],
                "abnormal": v["avg_duration"],
                "anomaly_score": avg_duration_result["total_score"],
                "change_rate": avg_duration_result["change_rate"],
                "absolute_change": avg_duration_result["absolute_change"],
                "z_score": avg_duration_result["z_score"],
                "slo_violated": True,
            }
            logger.debug(
                f"Duration anomaly detected for {k}: "
                f"score={avg_duration_result['total_score']:.3f}, "
                f"change={avg_duration_result['change_rate']:.1f}, "
                f"abs_change={avg_duration_result['absolute_change']:.1f}s"
            )

        sorted_durations = sorted(normal_durations)
        p90_start = int(len(sorted_durations) * 0.85)
        p90_end = int(len(sorted_durations) * 0.95)
        p90_normal_data = sorted_durations[p90_start:p90_end] if p90_start < p90_end else normal_durations

        if p90_normal_data:
            p90_result = calculate_anomaly_score(p90_normal_data, v["p90_duration"])
            if p90_result["is_anomaly"]:
                abnormal_tag["p90_duration"] = {
                    "normal": normal_stat[k]["p90_duration"],
                    "abnormal": v["p90_duration"],
                    "anomaly_score": p90_result["total_score"],
                    "change_rate": p90_result["change_rate"],
                    "absolute_change": p90_result["absolute_change"],
                    "slo_violated": True,
                }
                logger.debug(
                    f"P90 duration anomaly detected for {k}: "
                    f"score={p90_result['total_score']:.3f}, "
                    f"change={p90_result['change_rate']:.1f}, "
                    f"abs_change={p90_result['absolute_change']:.1f}"
                )

        p95_start = int(len(sorted_durations) * 0.90)
        p95_end = int(len(sorted_durations) * 0.99)
        p95_normal_data = sorted_durations[p95_start:p95_end] if p95_start < p95_end else normal_durations

        if p95_normal_data:
            p95_result = calculate_anomaly_score(p95_normal_data, v["p95_duration"])
            if p95_result["is_anomaly"]:
                abnormal_tag["p95_duration"] = {
                    "normal": normal_stat[k]["p95_duration"],
                    "abnormal": v["p95_duration"],
                    "anomaly_score": p95_result["total_score"],
                    "change_rate": p95_result["change_rate"],
                    "absolute_change": p95_result["absolute_change"],
                    "slo_violated": True,
                }
                logger.debug(
                    f"P95 duration anomaly detected for {k}: "
                    f"score={p95_result['total_score']:.3f}, "
                    f"change={p95_result['change_rate']:.1f}, "
                    f"abs_change={p95_result['absolute_change']:.1f}"
                )

        p99_start = int(len(sorted_durations) * 0.95)
        p99_normal_data = sorted_durations[p99_start:] if p99_start < len(sorted_durations) else normal_durations

        if p99_normal_data:
            p99_result = calculate_anomaly_score(p99_normal_data, v["p99_duration"])
            if p99_result["is_anomaly"]:
                abnormal_tag["p99_duration"] = {
                    "normal": normal_stat[k]["p99_duration"],
                    "abnormal": v["p99_duration"],
                    "anomaly_score": p99_result["total_score"],
                    "change_rate": p99_result["change_rate"],
                    "absolute_change": p99_result["absolute_change"],
                    "slo_violated": True,
                }
                logger.debug(
                    f"P99 duration anomaly detected for {k}: "
                    f"score={p99_result['total_score']:.3f}, "
                    f"change={p99_result['change_rate']:.1f}, "
                    f"abs_change={p99_result['absolute_change']:.1f}"
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

        if abnormal_tag:
            anomaly_count += 1

        conclusion_data.append(
            {
                "SpanName": k,
                "Issues": json.dumps(abnormal_tag),
                "AbnormalAvgDuration": v["avg_duration"],
                "NormalAvgDuration": normal_stat[k]["avg_duration"],
                "AbnormalSuccRate": abnormal_succ_rate,
                "NormalSuccRate": normal_succ_rate,
                "AbnormalP90": v["p90_duration"],
                "NormalP90": normal_stat[k]["p90_duration"],
                "AbnormalP95": v["p95_duration"],
                "NormalP95": normal_stat[k]["p95_duration"],
                "AbnormalP99": v["p99_duration"],
                "NormalP99": normal_stat[k]["p99_duration"],
            }
        )

    logger.info(f"Analysis complete. Found {anomaly_count} endpoints with anomalies")

    conclusion = pl.DataFrame(conclusion_data)
    conclusion.write_csv(Path(output_path) / "conclusion.csv")
    logger.info(f"Results saved to {Path(output_path) / 'conclusion.csv'}")

    if convert:
        try:
            platform_convert(in_p, ou_p)
        except Exception as e:
            logger.error(f"Error during platform conversion: {e}")
            raise


def platform_convert(in_p: Path | None = None, ou_p: Path | None = None):
    from rcabench_platform.v2.sources.convert import convert_datapack
    from rcabench_platform.v2.sources.rcabench import RcabenchDatapackLoader

    if in_p is None:
        in_p = Path(os.environ.get("INPUT_PATH", ""))
    if ou_p is None:
        ou_p = Path(os.environ.get("OUTPUT_PATH", ""))

    input_path = in_p
    output_path = ou_p
    assert input_path.exists()
    assert output_path.exists()

    with open(input_path / "injection.json") as f:
        injection = json.load(f)
        injection_name = injection["injection_name"]
        assert isinstance(injection_name, str) and injection_name

    converted_input_path = output_path / "converted"

    convert_datapack(
        loader=RcabenchDatapackLoader(src_folder=input_path, datapack=injection_name),
        dst_folder=converted_input_path,
        skip_finished=True,
    )


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

    tasks = []

    for datapack in input_path.iterdir():
        if not datapack.is_dir():
            continue

        trace_file = [datapack / "abnormal_traces.parquet", datapack / "normal_traces.parquet"]
        if not all(f.exists() for f in trace_file):
            logger.warning(f"Skipping {datapack.name} due to missing trace files")
            continue

        tasks.append(functools.partial(run, in_p=datapack, ou_p=datapack, convert=False))

    cpu = os.cpu_count()
    assert cpu is not None
    fmap_processpool(tasks, parallel=cpu // 4, cpu_limit_each=4)


@app.command()
def query_issues():
    input_path = Path("data") / "rcabench_dataset"

    datapacks = []
    errs = []
    for datapack in input_path.iterdir():
        if not datapack.is_dir():
            continue
        con = datapack / "conclusion.csv"

        if not con.exists():
            logger.warning(f"No conclusion found for {datapack.name}, skipping")
            continue

        logger.info(f"Processing {datapack} for issues")

        try:
            conclusion = pl.read_csv(con)

            non_empty_issues = conclusion.filter(
                (pl.col("Issues").is_not_null()) & (pl.col("Issues") != "") & (pl.col("Issues") != "{}")
            )
            if len(non_empty_issues) > 0:
                datapacks.append(datapack.name)
        except Exception as e:
            logger.error(f"Error processing {con}: {e}")
            errs.append((datapack.name, str(e)))
            continue

    logger.info(f"Found {len(datapacks)} datapacks with issues, skipping {len(errs)} with errors")
    return datapacks, errs


if __name__ == "__main__":
    app()
