#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.datasets.rcabench import rcabench_fix_injection_display_config
from rcabench_platform.v2.sources.convert import link_subset
from rcabench_platform.v2.utils.serde import save_parquet
from rcabench_platform.v2.utils.fmap import fmap_threadpool
from rcabench_platform.v2.datasets.spec import (
    get_datapack_folder,
    get_dataset_folder,
    get_dataset_meta_file,
    get_dataset_meta_folder,
)

from pathlib import Path
from typing import Any
import functools
import shutil
import json

import polars as pl


@app.command()
@timeit()
def run():
    lf = pl.scan_parquet(get_dataset_meta_file("rcabench", "attributes.parquet"))

    col = pl.col("files.total_size:MiB")
    lf = lf.filter(
        (col >= 10) & (col <= 500),
    )

    lf = lf.filter(
        pl.col("detector.conclusion.rows").is_not_null(),
        pl.col("detector.issues.rows") > 0,
    )

    unsuccessful_fault_types = (
        "DNSError",
        "DNSRandom",
        "JVMMySQLLatency",
        "JVMMySQLException",
    )
    lf = lf.filter(pl.col("injection.fault_type").is_in(unsuccessful_fault_types).not_())

    col = pl.col("injection.injection_point.class_name")
    lf = lf.filter(
        col.is_null() | (col.str.ends_with("Test") | col.str.ends_with("Config")).not_(),
    )

    col = pl.col("injection.rate")
    lf = lf.filter(
        col.is_null() | (col / 8000 < 20),
    )

    lf = lf.filter(
        pl.col("injection.mem_worker").is_null()
        | (pl.col("injection.mem_worker") * pl.col("injection.memory_size") >= 500)
    )

    col = pl.col("inject_time")
    lf = lf.filter(
        (col < pl.datetime(2025, 5, 27, 8, time_zone="UTC")) | (col > pl.datetime(2025, 5, 28, 0, time_zone="UTC")),
    )
    lf = lf.filter(
        (col < pl.datetime(2025, 5, 30, 4, time_zone="UTC")) | (col > pl.datetime(2025, 5, 30, 16, time_zone="UTC"))
    )

    df = lf.collect()

    tasks = []
    for row in df.select("datapack", "injection.fault_type", "injection.display_config").iter_rows(named=True):
        tasks.append(functools.partial(_check_datapack, row))
    results = fmap_threadpool(tasks, parallel=32)
    kickout_df = pl.DataFrame([r for r in results if r is not None])
    save_parquet(kickout_df, path=get_dataset_meta_file("rcabench", "kickout.parquet"))

    kickout_datapacks = kickout_df["datapack"].to_list()
    df = df.filter(pl.col("datapack").is_in(kickout_datapacks).not_())

    dataset = "rcabench_filtered"

    dataset_folder = get_dataset_folder(dataset)
    shutil.rmtree(dataset_folder, ignore_errors=True)

    datapacks = df["datapack"].to_list()
    link_subset(src_dataset="rcabench", dst_dataset=dataset, datapacks=datapacks)

    df = df.with_columns(pl.lit(dataset).alias("dataset"))

    meta_folder = get_dataset_meta_folder(dataset)
    save_parquet(df, path=meta_folder / "attributes.parquet")


def _check_datapack(row: dict[str, Any]) -> dict[str, Any] | None:
    datapack = row["datapack"]
    assert isinstance(datapack, str) and datapack

    datapack_folder = get_datapack_folder("rcabench", datapack)

    injection_fault_type = row["injection.fault_type"]
    assert isinstance(injection_fault_type, str)

    injection_config = json.loads(row["injection.display_config"])
    assert isinstance(injection_config, dict)

    rcabench_fix_injection_display_config(injection_config)

    injection_point = injection_config.get("injection_point")
    if not injection_point:
        return None
    assert isinstance(injection_point, dict)

    if injection_fault_type.startswith("Network"):
        direction = injection_point.get("direction")
        if direction is None:
            direction = injection_config.get("direction")
        assert direction in ["from", "to", "both"], injection_config

        source_service = injection_point.get("source_service")
        target_service = injection_point.get("target_service")
        if not source_service or not target_service:
            return None
        assert isinstance(source_service, str)
        assert isinstance(target_service, str)

        has_direct_calls = False

        if direction in ("from", "both"):
            has_direct_calls |= scan_direct_calls_from_traces(
                datapack_folder,
                source_service,
                target_service,
            )

        if direction in ("to", "both"):
            has_direct_calls |= scan_direct_calls_from_traces(
                datapack_folder,
                target_service,
                source_service,
            )

        if not has_direct_calls:
            logger.debug(
                f"datapack `{datapack}`: no direct calls between \
                    `{source_service}` and `{target_service}`, direction `{direction}`"
            )
            return {
                "datapack": datapack,
                "reason": f"{injection_fault_type};{scan_direct_calls_from_traces.__name__}",
            }

    if injection_fault_type == "HTTPRequestReplaceMethod":
        method = injection_point.get("method")
        replace_method = injection_config.get("replace_method")
        assert isinstance(method, str) and method
        assert isinstance(replace_method, str) and replace_method
        if method == replace_method:
            return {
                "datapack": datapack,
                "reason": f"{injection_fault_type};check replace_method",
            }

    if injection_fault_type.startswith("HTTP"):
        app_name = injection_point.get("app_name")
        server_address = injection_point.get("server_address")
        assert isinstance(app_name, str) and app_name
        assert isinstance(server_address, str) and server_address

        has_direct_calls = scan_direct_calls_from_traces(
            datapack_folder,
            source_service=app_name,
            target_service=server_address,
        )

        if not has_direct_calls:
            logger.debug(f"datapack `{datapack}`: no direct calls between `{app_name}` and `{server_address}`")
            return {
                "datapack": datapack,
                "reason": f"{injection_fault_type};{scan_direct_calls_from_traces.__name__}",
            }

    if is_single_point_failure(injection_fault_type):
        target_service = injection_point.get("app_label")
        if target_service is None:
            target_service = injection_point.get("app_name")
        assert isinstance(target_service, str) and target_service

        has_direct_calls = scan_direct_calls_from_traces(
            datapack_folder,
            None,
            target_service,
        )

        if not has_direct_calls:
            logger.debug(f"datapack `{datapack}`: no direct calls to `{target_service}`")
            return {
                "datapack": datapack,
                "reason": f"{injection_fault_type};{scan_direct_calls_from_traces.__name__}",
            }

    if scan_duplicated_spans(datapack_folder):
        logger.debug(f"datapack `{datapack}`: has duplicated spans")
        return {
            "datapack": datapack,
            "reason": scan_duplicated_spans.__name__,
        }


def is_single_point_failure(fault_type: str) -> bool:
    return fault_type.startswith("JVM") or fault_type in ("PodFailure", "CPUStress", "MemoryStress")


@timeit()
def scan_direct_calls_from_traces(
    datapack_folder: Path,
    source_service: str | None,
    target_service: str,
) -> bool:
    assert datapack_folder.exists()

    normal_traces = pl.scan_parquet(datapack_folder / "normal_traces.parquet")
    anomal_traces = pl.scan_parquet(datapack_folder / "abnormal_traces.parquet")
    traces = pl.concat([normal_traces, anomal_traces])

    lf = traces.select(
        "span_id",
        "parent_span_id",
        "service_name",
    )

    lf = lf.join(
        lf.select("span_id", pl.col("service_name").alias("parent_service_name")),
        left_on="parent_span_id",
        right_on="span_id",
        how="left",
    )

    if source_service:
        lf = lf.filter(
            pl.col("parent_service_name") == source_service,
            pl.col("service_name") == target_service,
        )
    else:
        lf = lf.filter(
            pl.col("service_name") == target_service,
        )

    df = lf.collect()

    logger.debug(f"source=`{source_service}`, target=`{target_service}`, len(df)={len(df)}")

    return len(df) > 0


@timeit()
def scan_duplicated_spans(datapack_folder: Path) -> bool:
    for file in ("normal_traces.parquet", "abnormal_traces.parquet"):
        lf = pl.scan_parquet(datapack_folder / file)
        lf = lf.select("span_id", "span_name", "parent_span_id")
        df = lf.collect()

        total_count = df.n_unique()
        id_count = df.select("span_id").n_unique()

        if len(df) != total_count or total_count != id_count:
            return True

    return False


if __name__ == "__main__":
    app()
