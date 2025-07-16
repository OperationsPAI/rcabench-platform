#!/usr/bin/env -S uv run -s
"""
Migrated from https://github.com/LGU-SE-Internal/ts-anomaly-detector
"""

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
    logger.info(datapacks[:20])
    return datapacks, errs


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
        if max_duration is not None and max_duration < du * 1e9:  # 1 second in nanoseconds
            return datapack_path.name

    except Exception as e:
        logger.error(f"Error processing {datapack_path.name}: {e}")
    return None


@app.command()
@timeit()
def query_with_confidence(duration: int):
    input_path = Path("data") / "rcabench_dataset"

    datapack_paths = [datapack for datapack in input_path.iterdir() if datapack.is_dir()]

    tasks = [
        functools.partial(process_datapack_confidence, datapack_path, duration) for datapack_path in datapack_paths
    ]

    # Process in parallel using thread pool (I/O bound operations)
    cpu = os.cpu_count()
    assert cpu is not None
    results = fmap_threadpool(tasks, parallel=min(cpu, 32))

    # Filter out None results
    datapacks = [result for result in results if result is not None]

    logger.info(f"Found {len(datapacks)} datapacks with all durations < {duration}s")

    return datapacks


def vis_call(datapack: Path):
    # First, check if conclusion.csv exists and get APIs with issues
    conclusion_file = datapack / "conclusion.csv"
    apis_with_issues = set()

    if conclusion_file.exists():
        try:
            conclusion = pl.read_csv(conclusion_file)
            # Filter APIs that have non-empty issues
            non_empty_issues = conclusion.filter(
                (pl.col("Issues").is_not_null()) & (pl.col("Issues") != "") & (pl.col("Issues") != "{}")
            )
            apis_with_issues = set(non_empty_issues["SpanName"].to_list())
            logger.info(f"Found {len(apis_with_issues)} APIs with issues to visualize")
        except Exception as e:
            logger.warning(f"Could not read conclusion.csv: {e}, visualizing all APIs")
    else:
        logger.warning("No conclusion.csv found, visualizing all APIs")

    df1 = pl.scan_parquet(datapack / "normal_traces.parquet").collect()
    df2 = pl.scan_parquet(datapack / "abnormal_traces.parquet").collect()

    df1 = df1.with_columns(pl.lit("normal").alias("trace_type"))
    df2 = df2.with_columns(pl.lit("abnormal").alias("trace_type"))

    merged_df = pl.concat([df1, df2])

    entry_df = merged_df.filter(pl.col("ServiceName") == "loadgenerator-service")

    entry_count = len(entry_df)
    if entry_count == 0:
        logger.error("loadgenerator-service not found in trace data, using ts-ui-dashboard as fallback")
        entry_df = merged_df.filter(pl.col("ServiceName") == "ts-ui-dashboard")
        entry_count = len(entry_df)

    if entry_count == 0:
        logger.error("No valid entrypoint found in trace data")
        return

    # Check timestamp data type once for the entire function
    timestamp_dtype = entry_df.select("Timestamp").dtypes[0]

    # Handle timestamp conversion - check if already datetime or needs conversion
    if timestamp_dtype == pl.Datetime:
        # Already datetime, use as is
        entry_df = entry_df.with_columns(
            [
                pl.col("Timestamp").alias("datetime"),
                (pl.col("Duration") / 1e9).alias("duration"),
            ]
        ).sort("Timestamp")
    else:
        # Assume nanosecond timestamp, convert to datetime
        entry_df = entry_df.with_columns(
            [
                pl.from_epoch(pl.col("Timestamp") // 1_000_000_000).alias("datetime"),
                (pl.col("Duration") / 1e9).alias("duration"),
            ]
        ).sort("Timestamp")

    entry_df = entry_df.with_columns(
        pl.col("SpanName").map_elements(extract_path, return_dtype=pl.Utf8).alias("api_path")
    )

    api_groups = entry_df.group_by("api_path")

    output_dir = Path("temp") / "vis" / datapack.name
    output_dir.mkdir(parents=True, exist_ok=True)

    processed_count = 0
    for api_path, group_df in api_groups:
        api_name = api_path[0] if isinstance(api_path, tuple) else str(api_path)

        # Skip APIs that don't have issues (if we have conclusion data)
        if apis_with_issues and api_name not in apis_with_issues:
            continue

        if len(group_df) < 10:
            continue

        group_df = group_df.sort("datetime")

        # Separate normal and abnormal data
        normal_data = group_df.filter(pl.col("trace_type") == "normal")
        abnormal_data = group_df.filter(pl.col("trace_type") == "abnormal")

        if len(normal_data) == 0 and len(abnormal_data) == 0:
            continue

        fig, ax = plt.subplots(figsize=(15, 8))

        # Plot normal data lines
        if len(normal_data) > 0:
            normal_times = normal_data["datetime"].to_list()
            normal_durations = normal_data["duration"].to_list()

            # Calculate moving statistics or use raw data points
            # For simplicity, we'll plot the raw duration points as lines
            ax.plot(
                normal_times,
                normal_durations,
                label="Normal - Duration",
                color="blue",
                alpha=0.7,
                linewidth=1,
                marker="o",
                markersize=2,
            )

            # Calculate and plot moving averages if there are enough points
            if len(normal_data) >= 10:
                # Create a rolling window calculation
                normal_sorted = normal_data.sort("datetime")
                window_size = max(5, len(normal_data) // 20)  # Adaptive window size

                # Calculate rolling statistics
                normal_with_stats = normal_sorted.with_columns(
                    [
                        pl.col("duration").rolling_mean(window_size).alias("avg_duration"),
                        pl.col("duration").rolling_quantile(0.9, window_size=window_size).alias("p90_duration"),
                        pl.col("duration").rolling_quantile(0.99, window_size=window_size).alias("p99_duration"),
                    ]
                )

                # Filter out null values from rolling calculations
                normal_with_stats = normal_with_stats.filter(pl.col("avg_duration").is_not_null())

                if len(normal_with_stats) > 0:
                    times = normal_with_stats["datetime"].to_list()
                    ax.plot(
                        times,
                        normal_with_stats["avg_duration"].to_list(),
                        label="Normal - Average",
                        color="blue",
                        alpha=0.8,
                        linewidth=2,
                    )
                    ax.plot(
                        times,
                        normal_with_stats["p90_duration"].to_list(),
                        label="Normal - P90",
                        color="green",
                        alpha=0.8,
                        linewidth=2,
                    )
                    ax.plot(
                        times,
                        normal_with_stats["p99_duration"].to_list(),
                        label="Normal - P99",
                        color="orange",
                        alpha=0.8,
                        linewidth=2,
                    )

        # Plot abnormal data lines
        if len(abnormal_data) > 0:
            abnormal_times = abnormal_data["datetime"].to_list()
            abnormal_durations = abnormal_data["duration"].to_list()

            # Plot raw duration points
            ax.plot(
                abnormal_times,
                abnormal_durations,
                label="Abnormal - Duration",
                color="red",
                alpha=0.7,
                linewidth=1,
                linestyle="--",
                marker="x",
                markersize=2,
            )

            # Calculate and plot moving averages if there are enough points
            if len(abnormal_data) >= 10:
                abnormal_sorted = abnormal_data.sort("datetime")
                window_size = max(5, len(abnormal_data) // 20)

                abnormal_with_stats = abnormal_sorted.with_columns(
                    [
                        pl.col("duration").rolling_mean(window_size).alias("avg_duration"),
                        pl.col("duration").rolling_quantile(0.9, window_size=window_size).alias("p90_duration"),
                        pl.col("duration").rolling_quantile(0.99, window_size=window_size).alias("p99_duration"),
                    ]
                )

                abnormal_with_stats = abnormal_with_stats.filter(pl.col("avg_duration").is_not_null())

                if len(abnormal_with_stats) > 0:
                    times = abnormal_with_stats["datetime"].to_list()
                    ax.plot(
                        times,
                        abnormal_with_stats["avg_duration"].to_list(),
                        label="Abnormal - Average",
                        color="red",
                        alpha=0.8,
                        linewidth=2,
                        linestyle="--",
                    )
                    ax.plot(
                        times,
                        abnormal_with_stats["p90_duration"].to_list(),
                        label="Abnormal - P90",
                        color="darkred",
                        alpha=0.8,
                        linewidth=2,
                        linestyle="--",
                    )
                    ax.plot(
                        times,
                        abnormal_with_stats["p99_duration"].to_list(),
                        label="Abnormal - P99",
                        color="maroon",
                        alpha=0.8,
                        linewidth=2,
                        linestyle="--",
                    )

        # Add vertical line to separate normal and abnormal periods
        if len(normal_data) > 0:
            normal_times = normal_data["datetime"].to_list()
            # Find the last time point in normal data
            last_normal_time = max(normal_times)
            ax.axvline(
                x=last_normal_time,
                color="black",
                linestyle="-",
                linewidth=2,
                alpha=0.8,
                label="Normal/Abnormal Boundary",
            )

        ax.set_xlabel("Time", fontsize=12)
        ax.set_ylabel("Latency (s)", fontsize=12)
        ax.set_title(f"Latency Time Series - {api_name}", fontsize=14, fontweight="bold")
        ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
        ax.grid(True, alpha=0.3)

        # Format x-axis with minute granularity
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        plt.tight_layout()

        safe_filename = re.sub(r"[^\w\-_.]", "_", str(api_name))
        output_file = output_dir / f"{safe_filename}_latency_timeseries.png"

        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()

        logger.info(f"Saved latency time series plot for {api_name} to {output_file}")
        processed_count += 1

    logger.info(f"Processed {processed_count} APIs with issues, all visualization plots saved to {output_dir}")


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


@app.command()
@timeit()
def batch_visualize():
    datapacks, _ = query_issues()
    for datapack_path in datapacks:
        try:
            logger.info(f"Processing {datapack_path}")
            vis_call(Path("data/rcabench_dataset") / datapack_path)
        except Exception as e:
            logger.error(f"Error processing {datapack_path}: {e}")
            continue

    logger.info("Batch visualization completed")


if __name__ == "__main__":
    app()
