#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.clients.rcabench_ import get_mariadb_connection
from rcabench_platform.v1.datasets.rcabench_ import FAULT_TYPES
from rcabench_platform.v1.datasets.convert_rcabench import RcabenchDatapackLoader, RcabenchDatasetLoader
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.spec.convert import convert_datapack, convert_dataset
from rcabench_platform.v1.spec.data import DATA_ROOT, META_ROOT, TEMP, get_datapack_list
from rcabench_platform.v1.utils.dataframe import print_dataframe
from rcabench_platform.v1.utils.dict_ import flatten_dict
from rcabench_platform.v1.utils.fmap import fmap_threadpool
from rcabench_platform.v1.utils.serde import load_json, save_json, save_parquet

from fractions import Fraction
from pathlib import Path
from typing import Any
import functools
import datetime
import json

import polars as pl
from tqdm.auto import tqdm
import dateutil.tz


@app.command()
@timeit()
def run(skip: bool = True, parallel: int | None = 4, scan: bool = True):
    src_root = Path("data") / "rcabench_dataset"

    loader = RcabenchDatasetLoader(src_root, dataset="rcabench")

    convert_dataset(
        loader,
        skip=skip,
        parallel=parallel,
        ignore_exceptions=True,
    )

    if scan:
        scan_datapack_attributes()


@app.command()
@timeit()
def local_test_1():
    datapack = "ts0-ts-preserve-service-response-delay-jqrgnc"
    loader = RcabenchDatapackLoader(
        src_folder=Path("data") / "rcabench_dataset" / datapack,
        datapack=datapack,
    )
    convert_datapack(
        loader,
        dst_folder=Path("temp") / "rcabench" / datapack,
        skip=False,
    )


@app.command()
@timeit()
def scan_datapack_attributes():
    dataset = "rcabench"
    datapacks = get_datapack_list(dataset)

    tasks = []
    for datapack, input_folder in datapacks:
        tasks.append(functools.partial(_task_scan_datapack_attributes, dataset, datapack, input_folder))

    results = fmap_threadpool(tasks, parallel=32)

    df = pl.DataFrame(results).sort("inject_time", descending=True)

    save_parquet(df, path=META_ROOT / dataset / "attributes.parquet")


def _convert_time(ts: int, tz: datetime.tzinfo | None) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts, tz=datetime.UTC).replace(tzinfo=tz).astimezone(datetime.UTC)


def _task_scan_datapack_attributes(dataset: str, datapack: str, input_folder: Path) -> dict[str, Any]:
    attrs: dict[str, Any] = {"dataset": dataset, "datapack": datapack}

    env = load_json(path=input_folder / "env.json")
    injection = load_json(path=input_folder / "injection.json")

    tz = dateutil.tz.gettz(env["TIMEZONE"])
    normal_start = _convert_time(int(env["NORMAL_START"]), tz)
    normal_end = _convert_time(int(env["NORMAL_END"]), tz)
    abnormal_start = _convert_time(int(env["ABNORMAL_START"]), tz)
    abnormal_end = _convert_time(int(env["ABNORMAL_END"]), tz)

    attrs["inject_time"] = abnormal_start

    display_config = flatten_dict(json.loads(injection["display_config"]))
    attrs["injection.fault_type"] = FAULT_TYPES[injection["fault_type"]]
    attrs["injection.display_config"] = injection["display_config"]
    attrs["injection.duration"] = display_config["duration"]

    configs = [
        "injection_point.class_name",
        "rate",
        "mem_worker",
        "memory_size",
    ]
    for config in configs:
        attrs[f"injection.{config}"] = display_config.get(config)

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
def make_filtered():
    lf = pl.scan_parquet(META_ROOT / "rcabench" / "attributes.parquet")

    col = pl.col("files.total_size:MiB")
    lf = lf.filter(
        (col >= 10) & (col <= 500),
    )

    lf = lf.filter(
        pl.col("detector.conclusion.rows").is_not_null(),
        pl.col("detector.issues.rows") > 0,
    )

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
    save_parquet(kickout_df, path=META_ROOT / "rcabench" / "kickout.parquet")

    kickout_datapacks = kickout_df["datapack"].to_list()
    df = df.filter(pl.col("datapack").is_in(kickout_datapacks).not_())

    dataset = "rcabench_filtered"

    dataset_folder = DATA_ROOT / dataset
    dataset_folder.mkdir(parents=True, exist_ok=True)

    datapacks = df["datapack"].to_list()
    for datapack in tqdm(datapacks):
        assert isinstance(datapack, str) and datapack

        src_folder = Path("..") / "rcabench" / datapack
        dst_folder = dataset_folder / datapack

        dst_folder.unlink(missing_ok=True)
        dst_folder.symlink_to(src_folder, target_is_directory=True)

    index_df = df.select("dataset", "datapack")

    label_df = pl.read_parquet(META_ROOT / "rcabench" / "label.parquet").join(
        df.select("datapack"),
        on="datapack",
        how="inner",
    )

    col = pl.lit(dataset).alias("dataset")
    df = df.with_columns(col)
    index_df = index_df.with_columns(col)
    label_df = label_df.with_columns(col)

    save_parquet(df, path=META_ROOT / dataset / "attributes.parquet")
    save_parquet(index_df, path=META_ROOT / dataset / "index.parquet")
    save_parquet(label_df, path=META_ROOT / dataset / "label.parquet")


def _check_datapack(row: dict[str, Any]) -> dict[str, Any] | None:
    datapack = row["datapack"]
    assert isinstance(datapack, str) and datapack

    datapack_folder = DATA_ROOT / "rcabench" / datapack

    injection_fault_type = row["injection.fault_type"]
    assert isinstance(injection_fault_type, str)

    injection_config = json.loads(row["injection.display_config"])
    assert isinstance(injection_config, dict)

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
                f"datapack `{datapack}`: no direct calls between `{source_service}` and `{target_service}`, direction `{direction}`"
            )
            return {
                "datapack": datapack,
                "reason": f"{injection_fault_type};{scan_direct_calls_from_traces.__name__}",
            }

    elif injection_fault_type.startswith("HTTP"):
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

    single_point_failures = (
        "PodFailure",
        "CPUStress",
        "MemoryStress",
        "JVMCPUStress",
        "JVMMemoryStress",
    )

    if injection_fault_type in single_point_failures:
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


# @app.command()
# @timeit()
# def scan_conclusion_csv(include_legacy: bool = False):
#     root = Path("data") / "rcabench_dataset"
#     folders = [f.name for f in root.iterdir() if f.is_dir()]

#     conclusion_df_list = []
#     for datapack in tqdm(folders):
#         conclusion_csv_path = root / datapack / "conclusion.csv"
#         if not conclusion_csv_path.exists():
#             continue

#         if not include_legacy:
#             if not (root / datapack / "injection.json").exists():
#                 continue

#         conclusion_df = pl.read_csv(conclusion_csv_path)
#         if conclusion_df.is_empty():
#             continue

#         columns = conclusion_df.columns
#         conclusion_df = conclusion_df.select(pl.lit(datapack).alias("datapack"), *columns)

#         conclusion_df_list.append(conclusion_df)

#     df = pl.concat(conclusion_df_list)
#     save_parquet(df, path=META_ROOT / "rcabench" / "conclusion.parquet")


@app.command()
@timeit()
def merge_conclusion():
    datapacks = get_datapack_list("rcabench")

    df_list = []
    for datapack_name, datapack_path in tqdm(datapacks):
        if not datapack_path.is_dir():
            continue

        conclusion_path = datapack_path / "conclusion.parquet"
        if not conclusion_path.exists():
            continue

        df = pl.read_parquet(conclusion_path)

        if df.is_empty():
            continue

        df = df.select(pl.lit(datapack_name).alias("datapack"), pl.all())
        df_list.append(df)

    df = pl.concat(df_list)
    save_parquet(df, path=META_ROOT / "rcabench" / "conclusion.parquet")


@app.command()
@timeit()
def make_with_issues(db_only: bool = False, require_filtered: bool = False):
    with get_mariadb_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT injection_name, fault_type \
            FROM fault_injection_with_issues \
            GROUP BY injection_name, fault_type \
            ORDER BY injection_name ASC;"
        )

        rows = cursor.fetchall()
        df = pl.DataFrame(rows)

    save_parquet(df, path=META_ROOT / "rcabench" / "with_issues.db.parquet")

    if db_only:
        return

    if require_filtered:
        filtered_df = (
            pl.read_parquet(META_ROOT / "rcabench_filtered" / "index.parquet")
            .select("datapack")
            .rename({"datapack": "injection_name"})
        )
        df = df.join(filtered_df, on="injection_name", how="inner")

    datapacks = df["injection_name"].to_list()

    dataset = "rcabench_with_issues"

    dataset_folder = DATA_ROOT / dataset
    dataset_folder.mkdir(parents=True, exist_ok=True)

    for datapack in tqdm(datapacks):
        assert isinstance(datapack, str) and datapack

        src_folder = Path("..") / "rcabench" / datapack
        dst_folder = dataset_folder / datapack

        dst_folder.unlink(missing_ok=True)
        dst_folder.symlink_to(src_folder, target_is_directory=True)

    index_df = df.select(pl.lit(dataset).alias("dataset"), pl.col("injection_name").alias("datapack"))

    label_df = (
        pl.read_parquet(META_ROOT / "rcabench" / "label.parquet")
        .join(index_df, on="datapack", how="inner")
        .with_columns(pl.lit(dataset).alias("dataset"))
    )

    save_parquet(index_df, path=META_ROOT / dataset / "index.parquet")
    save_parquet(label_df, path=META_ROOT / dataset / "label.parquet")

    query_with_issues_ratio()


@app.command()
@timeit()
def query_with_issues_ratio():
    with_issues = pl.read_parquet(META_ROOT / "rcabench_with_issues" / "index.parquet").select("datapack")
    filtered = pl.read_parquet(META_ROOT / "rcabench_filtered" / "index.parquet").select("datapack")

    joint_df = with_issues.join(filtered, on="datapack", how="inner")

    ratio = Fraction(len(joint_df), len(with_issues))
    logger.info(f"with_issues ratio: {len(joint_df)}/{len(with_issues)} {float(ratio):.2%}")


@app.command()
@timeit()
def query_fault_types(dataset: str):
    if dataset in ("rcabench", "rcabench_filtered"):
        lf = pl.scan_parquet(META_ROOT / dataset / "attributes.parquet")
        col = "injection.fault_type"
        df = lf.select(col).collect()
    elif dataset == "rcabench_with_issues":
        lf = pl.scan_parquet(META_ROOT / "rcabench" / "with_issues.db.parquet")
        col = "fault_type"
        replacement = {i: v for i, v in enumerate(FAULT_TYPES)}
        df = lf.select(col).collect().select(pl.col(col).replace_strict(replacement, return_dtype=pl.String))
    else:
        raise NotImplementedError

    fault_types_count = df[col].value_counts().sort("count", descending=True)
    save_parquet(fault_types_count, path=META_ROOT / dataset / "fault_types.count.parquet")

    print_dataframe(fault_types_count)


# @app.command()
# def patch_conclusion_files():
#     src = Path("data") / "rcabench_dataset"

#     datapacks = get_datapack_list("rcabench")

#     for datapack_name, datapack_path in tqdm(datapacks):
#         if not datapack_path.is_dir():
#             continue

#         src_path = src / datapack_name / "conclusion.csv"
#         dst_path = datapack_path / "conclusion.parquet"

#         if not src_path.exists():
#             continue

#         df = convert_conclusion(src_path).collect()
#         save_parquet(df, path=dst_path)


if __name__ == "__main__":
    app()
