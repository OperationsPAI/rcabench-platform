#!/usr/bin/env -S uv run -s
"""
Migrated from https://github.com/LGU-SE-Internal/ts-anomaly-detector
"""

from rcabench_platform.v2.cli.main import app, logger, timeit

from pathlib import Path
import json
import re
import os

import pandas as pd


def avg_duration(abnormal: float, normal: float) -> bool:
    return normal > 0.5 and abnormal > normal * 1.3


def p90_duration(abnormal: float, normal: float) -> bool:
    return normal > 1.5 and abnormal > normal * 1.5


def p95_duration(abnormal: float, normal: float) -> bool:
    return normal > 2 and abnormal > normal * 1.5


def p99_duration(abnormal: float, normal: float) -> bool:
    return normal > 2 and abnormal > normal * 2


def success_rate(abnormal: float, normal: float) -> bool:
    return (normal - abnormal) > normal * 0.9


def extract_path(uri: str):
    from rcabench_platform.v2.datasets.train_ticket import PATTERN_REPLACEMENTS

    for pattern, replacement in PATTERN_REPLACEMENTS:
        res = re.sub(pattern, replacement, uri)
        if res != uri:
            return res
    return uri


def read_dataframe(file: Path) -> pd.DataFrame:
    logger.info(f"Reading from parquet file: `{file}`")
    return pd.read_parquet(file)


def preprocess_trace(file: Path):
    logger.info(f"Starting preprocessing of trace file: {file}")
    df = read_dataframe(file)
    logger.info(f"Loaded trace file with {len(df)} records")

    df = df[df["ServiceName"] == "ts-ui-dashboard"]
    logger.info(f"Filtered to {len(df)} records with ServiceName='ts-ui-dashboard'")

    entrypoints = set(df["SpanName"])
    logger.info(f"Found {len(entrypoints)} unique endpoints")

    df["ts"] = df["Timestamp"]
    df = df.sort_values(by="ts", ascending=True)

    logger.info("Deduplicating entrypoints...")
    deduped_entrypoints = {}
    for entrypoint in entrypoints:
        path = extract_path(entrypoint)
        deduped_entrypoints[entrypoint] = path
    logger.info(f"Deduplication complete, found {len(set(deduped_entrypoints.values()))} unique paths")

    logger.info("Processing trace data and computing statistics...")
    stat = {}
    for _, row in df.iterrows():
        span_name = deduped_entrypoints.get(row["SpanName"], row["SpanName"])
        if span_name not in stat:
            stat[span_name] = {
                "timestamp": [],
                "duration": [],
                "status_code": [],
                "response_content_length": [],
                "request_content_length": [],
            }
        stat[span_name]["timestamp"].append(row["Timestamp"])
        stat[span_name]["duration"].append(row["Duration"])

        # not sure to take this or resource attributes
        # now we take resource attributes
        ra = json.loads(row["SpanAttributes"])
        if "http.status_code" in ra:
            stat[span_name]["status_code"].append(ra["http.status_code"])
        elif row["StatusCode"] != "Unset":
            # print("status code not found: ", row["StatusCode"])
            stat[span_name]["status_code"].append(row["StatusCode"])

        if "http.response_content_length" in ra:
            stat[span_name]["response_content_length"].append(ra["http.response_content_length"])
        if "http.request_content_length" in ra:
            stat[span_name]["request_content_length"].append(ra["http.request_content_length"])
    logger.info("Computing duration metrics and statistics...")
    for k, v in stat.items():
        avg_duration = sum(v["duration"]) / len(v["duration"])
        p90_duration = sorted(v["duration"])[int(len(v["duration"]) * 0.9)]
        p95_duration = sorted(v["duration"])[int(len(v["duration"]) * 0.95)]
        p99_duration = sorted(v["duration"])[int(len(v["duration"]) * 0.99)]
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

        # visual(k, v)
    logger.info(f"Preprocessing complete. Computed statistics for {len(stat)} endpoints")
    return stat


@app.command()
@timeit()
def run():
    logger.info("Starting RCA analysis")

    input_path = Path(os.environ["INPUT_PATH"])
    assert input_path.exists()

    output_path = os.environ["OUTPUT_PATH"]
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logger.info(f"Created output directory: {output_path}")

    normal_trace = Path(input_path) / "normal_traces.parquet"
    abnormal_trace = Path(input_path) / "abnormal_traces.parquet"

    logger.info(f"Processing normal trace file: {normal_trace}")
    normal_stat = preprocess_trace(normal_trace)
    logger.info(f"Processing abnormal trace file: {abnormal_trace}")
    abnormal_stat = preprocess_trace(abnormal_trace)

    logger.info("Beginning comparison between normal and abnormal traces")
    columns = [
        "SpanName",
        "Issues",
        "AbnormalAvgDuration",
        "NormalAvgDuration",
        "AbnormalSuccRate",
        "NormalSuccRate",
        "AbnormalP90",
        "NormalP90",
        "AbnormalP95",
        "NormalP95",
        "AbnormalP99",
        "NormalP99",
    ]
    conclusion = pd.DataFrame(columns=columns)

    anomaly_count = 0
    for k, v in abnormal_stat.items():
        if k not in normal_stat:
            logger.warning(f"New endpoint found: {k} - skipping comparison")
            continue

        logger.debug(f"Analyzing endpoint: {k}")
        abnormal_tag = {}
        if avg_duration(v["avg_duration"], normal_stat[k]["avg_duration"]):
            abnormal_tag["avg_duration"] = {
                "normal": normal_stat[k]["avg_duration"],
                "abnormal": v["avg_duration"],
                "ratio": v["avg_duration"] / normal_stat[k]["avg_duration"],
            }
            logger.debug(
                f"Anomaly detected in avg_duration for {k}: "
                f"ratio={v['avg_duration'] / normal_stat[k]['avg_duration']:.2f}"
            )

        if p90_duration(v["p90_duration"], normal_stat[k]["p90_duration"]):
            abnormal_tag["p90_duration"] = {
                "normal": normal_stat[k]["p90_duration"],
                "abnormal": v["p90_duration"],
                "ratio": v["p90_duration"] / normal_stat[k]["p90_duration"],
            }
            logger.debug(
                f"Anomaly detected in p90_duration for {k}: "
                f"ratio={v['p90_duration'] / normal_stat[k]['p90_duration']:.2f}"
            )

        if p95_duration(v["p95_duration"], normal_stat[k]["p95_duration"]):
            abnormal_tag["p95_duration"] = {
                "normal": normal_stat[k]["p95_duration"],
                "abnormal": v["p95_duration"],
                "ratio": v["p95_duration"] / normal_stat[k]["p95_duration"],
            }
            logger.debug(
                f"Anomaly detected in p95_duration for {k}: "
                f"ratio={v['p95_duration'] / normal_stat[k]['p95_duration']:.2f}"
            )

        if p99_duration(v["p99_duration"], normal_stat[k]["p99_duration"]):
            abnormal_tag["p99_duration"] = {
                "normal": normal_stat[k]["p99_duration"],
                "abnormal": v["p99_duration"],
                "ratio": v["p99_duration"] / normal_stat[k]["p99_duration"],
            }
            logger.debug(
                f"Anomaly detected in p99_duration for {k}: "
                f"ratio={v['p99_duration'] / normal_stat[k]['p99_duration']:.2f}"
            )

        normal_succ_rate = normal_stat[k]["status_code"].get("200", 0) / (
            sum(normal_stat[k]["status_code"].values()) + 1e-9
        )
        abnormal_succ_rate = v["status_code"].get("200", 0) / (sum(v["status_code"].values()) + 1e-9)
        if success_rate(abnormal_succ_rate, normal_succ_rate):
            abnormal_tag["succ_rate"] = {
                "normal": normal_succ_rate,
                "abnormal": abnormal_succ_rate,
            }
            logger.debug(
                f"Anomaly detected in success rate for {k}: "
                f"normal={normal_succ_rate:.2f}, abnormal={abnormal_succ_rate:.2f}"
            )

        # Increment anomaly counter only if there are issues
        if abnormal_tag:
            anomaly_count += 1

        # Always add to conclusion regardless of whether there are anomalies
        conclusion.loc[len(conclusion)] = [
            k,
            json.dumps(abnormal_tag),
            v["avg_duration"],
            normal_stat[k]["avg_duration"],
            abnormal_succ_rate,
            normal_succ_rate,
            v["p90_duration"],
            normal_stat[k]["p90_duration"],
            v["p95_duration"],
            normal_stat[k]["p95_duration"],
            v["p99_duration"],
            normal_stat[k]["p99_duration"],
        ]

    logger.info(f"Analysis complete. Found {anomaly_count} endpoints with anomalies")
    output_file = os.path.join(os.environ["OUTPUT_PATH"], "conclusion.csv")

    conclusion.to_csv(Path(output_path) / "conclusion.csv", index=False)

    logger.info(f"Results saved to {output_file}")

    try:
        platform_convert()
    except Exception as e:
        logger.error(f"Error during platform conversion: {e}")
        raise


def platform_convert():
    from rcabench_platform.v2.sources.convert import convert_datapack
    from rcabench_platform.v2.sources.rcabench import RcabenchDatapackLoader

    input_path = Path(os.environ["INPUT_PATH"])
    output_path = Path(os.environ["OUTPUT_PATH"])
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


if __name__ == "__main__":
    app()
