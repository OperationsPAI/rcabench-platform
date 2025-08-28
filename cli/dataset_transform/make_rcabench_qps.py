#!/usr/bin/env -S uv run -s
import datetime
import functools
import json
import math

#!/usr/bin/env -S uv run -s
import shutil
import time
from functools import wraps
from pathlib import Path
from typing import Any, Dict

import dateutil.tz
import matplotlib.pyplot as plt
import polars as pl
from matplotlib.axes import Axes
from rcabench.openapi import DatasetsApi, DtoDatasetV2CreateReq, DtoInjectionRef, InjectionsApi
from tqdm.auto import tqdm

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.config import get_config
from rcabench_platform.v2.datasets.rcabench import FAULT_TYPES, rcabench_split_train_test, valid
from rcabench_platform.v2.datasets.spec import (
    delete_dataset,
    get_datapack_folder,
    get_datapack_labels,
    get_datapack_list,
    get_dataset_meta_file,
)
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.logging import logger
from rcabench_platform.v2.sources.convert import (
    convert_datapack,
    convert_dataset,
    link_subset,
)
from rcabench_platform.v2.sources.rcabench import (
    RcabenchDatapackLoader,
    RcabenchDatasetLoader,
)
from rcabench_platform.v2.utils.dataframe import print_dataframe
from rcabench_platform.v2.utils.dict_ import flatten_dict
from rcabench_platform.v2.utils.fmap import fmap_processpool, fmap_threadpool
from rcabench_platform.v2.utils.serde import load_json, save_parquet


def ui_span_name_parser(df: pl.DataFrame) -> pl.DataFrame:
    """Parse span names for specific services using extract_path function"""
    # Process span names for loadgenerator and ts-ui-dashboard services
    processed_df = df.with_columns(
        [pl.col("SpanName").map_elements(extract_path, return_dtype=pl.String).alias("span_name")]
    )

    return processed_df.drop("SpanName")


@timeit()
def load_traces(input_folder: Path):
    """Load trace data from parquet files"""
    normal_traces = pl.scan_parquet(input_folder / "normal_traces.parquet")
    anomal_traces = pl.scan_parquet(input_folder / "abnormal_traces.parquet")
    dfs = []
    for lf in [normal_traces, anomal_traces]:
        lf = lf.filter(pl.col("ServiceName") == "loadgenerator")

        # Apply UI span name parsing
        df = lf.collect()
        df = ui_span_name_parser(df)
        dfs.append(df)

    return dfs


def get_datapack(tag: str = "no_anomaly") -> list[str]:
    with RCABenchClient(base_url=get_config().base_url) as client:
        injection_api = InjectionsApi(client)
        resp = injection_api.api_v2_injections_get(tags=[tag], page=1, size=10000)
        assert resp.code is not None and resp.code < 300 and resp.data is not None and resp.data.items is not None
        return [
            item.injection_name
            for item in resp.data.items
            if item.injection_name is not None and valid(Path("data") / "rcabench_dataset" / item.injection_name)
        ]


def _task_analyze_datapack(datapack_name: str) -> tuple[str, bool]:
    """
    Task function to analyze a single datapack for anomalous patterns.
    Returns tuple of (datapack_name, is_anomalous).
    """
    try:
        input_folder = Path("data") / "rcabench_dataset" / datapack_name
        if not input_folder.exists():
            return datapack_name, False

        # Load traces
        normal_df, abnormal_df = load_traces(input_folder)

        # Filter for loadgenerator service only
        normal_loadgen = normal_df
        abnormal_loadgen = abnormal_df

        if normal_loadgen.height == 0 or abnormal_loadgen.height == 0:
            return datapack_name, False

        # Count unique span names
        normal_span_counts = dict(normal_loadgen["span_name"].value_counts().iter_rows())
        abnormal_span_counts = dict(abnormal_loadgen["span_name"].value_counts().iter_rows())

        total_normal_count = normal_loadgen.height

        # Check for anomalous patterns
        for span_name, normal_count in normal_span_counts.items():
            abnormal_count = abnormal_span_counts.get(span_name, 0)
            normal_percentage = normal_count / total_normal_count

            # Skip if abnormal_count is 0 and normal percentage is less than 5%
            if normal_percentage < 0.05:
                continue

            # Check if abnormal count is 0 or less than 1/10 of normal count
            if (
                abnormal_count == 0
                or abnormal_count < normal_count / 10
                or abnormal_count < normal_count / 5
                and normal_percentage > 0.1
            ):
                logger.info(f"Found anomalous datapack: {datapack_name}")
                return datapack_name, True

        return datapack_name, False

    except Exception as e:
        logger.error(f"Error processing datapack {datapack_name}: {e}")
        return datapack_name, False


def analyze_anomalous_datapacks(parallel: int = 16) -> list[str]:
    """
    Analyze datapacks to find those with potentially anomalous span name patterns.
    Returns list of datapack names where any span name in abnormal phase has
    count < normal_count/10 or count == 0.
    """
    datapacks = get_datapack()

    tasks = [functools.partial(_task_analyze_datapack, datapack_name) for datapack_name in datapacks]

    results = fmap_processpool(tasks, parallel=parallel)

    anomalous_datapacks = [datapack_name for datapack_name, is_anomalous in results if is_anomalous]

    return anomalous_datapacks


@app.command()
@timeit()
def create_rcabench_qps(skip_finished: bool = True, parallel: int = 64):
    """Create rcabench_qps dataset from anomalous datapacks"""
    # Get anomalous datapacks
    anomalous_datapacks = analyze_anomalous_datapacks(parallel=parallel)
    print(f"Found {len(anomalous_datapacks)} anomalous datapacks for rcabench_qps")

    if not anomalous_datapacks:
        print("No anomalous datapacks found, skipping dataset creation")
        return

    dataset = "rcabench_qps"

    shutil.rmtree(Path("data") / "rcabench-platform-v2" / "data" / dataset, ignore_errors=True)
    tasks = []

    for datapack_name in tqdm(anomalous_datapacks, desc="Creating rcabench_qps datapacks"):
        loader = RcabenchDatapackLoader(
            src_folder=Path("data") / "rcabench_dataset" / datapack_name,
            datapack=datapack_name,
        )

        tasks.append(
            functools.partial(
                convert_datapack,
                loader,
                dst_folder=Path("data") / "rcabench-platform-v2" / "data" / "rcabench_qps" / datapack_name,
                skip_finished=skip_finished,
            )
        )

    results = fmap_processpool(
        tasks,
        parallel=parallel,
    )

    index_rows = []
    labels_rows = []
    for datapack, labels in results:
        index = {"dataset": dataset, "datapack": datapack}
        index_rows.append(index)
        for label in labels:
            labels_rows.append({**index, "gt.level": label.level, "gt.name": label.name})

    index_df = pl.DataFrame(index_rows).sort(by=pl.all())
    labels_df = pl.DataFrame(labels_rows).sort(by=pl.all())

    meta_folder = Path("data") / "rcabench-platform-v2" / "meta" / dataset
    meta_folder.mkdir(parents=True, exist_ok=True)
    save_parquet(index_df, path=meta_folder / "index.parquet")
    save_parquet(labels_df, path=meta_folder / "labels.parquet")

    print(f"Created rcabench_qps dataset with {len(index_rows)} datapacks")
    scan_datapack_attributes()


def _convert_time(ts: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)


def _task_scan_datapack_attributes(dataset: str, datapack: str, input_folder: Path) -> dict[str, Any]:
    attrs: dict[str, Any] = {"dataset": dataset, "datapack": datapack}

    env = load_json(path=input_folder / "env.json")
    injection = load_json(path=input_folder / "injection.json")

    # tz = dateutil.tz.gettz(env["TIMEZONE"])
    normal_start = _convert_time(int(env["NORMAL_START"]))
    normal_end = _convert_time(int(env["NORMAL_END"]))
    abnormal_start = _convert_time(int(env["ABNORMAL_START"]))
    abnormal_end = _convert_time(int(env["ABNORMAL_END"]))

    attrs["inject_time"] = abnormal_start

    display_config = flatten_dict(json.loads(injection["display_config"]))
    attrs["injection.fault_type"] = FAULT_TYPES[injection["fault_type"]]
    attrs["injection.display_config"] = injection["display_config"]
    attrs["injection.duration"] = display_config["duration"]

    # Ensure data type consistency, avoid type conflicts when creating Polars DataFrame
    configs = [
        ("injection_point.class_name", str, None),
        ("rate", int, None),
        ("mem_worker", int, None),
        ("memory_size", int, None),
    ]
    for config_name, expected_type, default_value in configs:
        value = display_config.get(config_name)
        if value is None:
            attrs[f"injection.{config_name}"] = default_value
        else:
            try:
                # Try to convert to expected type
                attrs[f"injection.{config_name}"] = expected_type(value) if expected_type is not str else str(value)
            except (ValueError, TypeError):
                # If conversion fails, use default value
                attrs[f"injection.{config_name}"] = default_value

    attrs["env.normal_start"] = normal_start
    attrs["env.normal_end"] = normal_end
    attrs["env.abnormal_start"] = abnormal_start
    attrs["env.abnormal_end"] = abnormal_end

    total_size = 0
    for file in input_folder.iterdir():
        if not file.is_file():
            continue
        total_size += file.stat().st_size
    attrs["files.total_size:MiB"] = round(total_size / (1024 * 1024), 6)

    conclusion_path = input_folder / "conclusion.parquet"
    if conclusion_path.exists():
        conclusion_df = pl.read_parquet(conclusion_path)
        attrs["detector.conclusion.rows"] = len(conclusion_df)
        attrs["detector.issues.rows"] = len(conclusion_df.filter(pl.col("Issues") != "{}"))

    return attrs


@app.command()
@timeit()
def scan_datapack_attributes():
    dataset = "rcabench_qps"
    datapacks = get_datapack_list(dataset)

    tasks = []
    for datapack in datapacks:
        input_folder = get_datapack_folder(dataset, datapack)
        tasks.append(functools.partial(_task_scan_datapack_attributes, dataset, datapack, input_folder))

    results = fmap_threadpool(tasks, parallel=32)

    # Explicitly specify schema to avoid type inference issues
    schema = {
        "dataset": pl.String,
        "datapack": pl.String,
        "inject_time": pl.Datetime,
        "injection.fault_type": pl.String,
        "injection.display_config": pl.String,
        "injection.duration": pl.Int64,
        "injection.injection_point.class_name": pl.String,
        "injection.rate": pl.Int64,
        "injection.mem_worker": pl.Int64,
        "injection.memory_size": pl.Int64,
        "env.normal_start": pl.Datetime,
        "env.normal_end": pl.Datetime,
        "env.abnormal_start": pl.Datetime,
        "env.abnormal_end": pl.Datetime,
        "files.total_size:MiB": pl.Float64,
        "detector.conclusion.rows": pl.Int64,
        "detector.issues.rows": pl.Int64,
    }

    df = pl.DataFrame(results, schema=schema).sort("inject_time", descending=True)

    save_parquet(df, path=get_dataset_meta_file(dataset, "attributes.parquet"))


if __name__ == "__main__":
    app()
