#!/usr/bin/env -S uv run -s
import datetime
import functools
import json
import shutil
from pathlib import Path
from typing import Any

import dateutil.tz
import polars as pl
from tqdm.auto import tqdm

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.datasets.rcabench import FAULT_TYPES
from rcabench_platform.v2.datasets.spec import (
    get_datapack_folder,
    get_datapack_list,
    get_dataset_meta_file,
)
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


@app.command()
@timeit()
def run(skip_finished: bool = True, parallel: int = 4, scan: bool = True):
    src_root = Path("data") / "rcabench_dataset"
    # filter sub dir containe exception
    datapacks = src_root.glob("*exception*")
    datapacks = [p for p in datapacks if p.is_dir()]
    print(f"Found {len(datapacks)} exception datapacks")
    dataset = "rcabench_execption"
    tasks = []
    for datapack_path in tqdm(datapacks, desc="Datapacks"):
        loader = RcabenchDatapackLoader(
            src_folder=Path("data") / "rcabench_dataset" / datapack_path.name,
            datapack=datapack_path.name,
        )

        tasks.append(
            functools.partial(
                convert_datapack,
                loader,
                dst_folder=Path("data") / "rcabench-platform-v2" / "data" / "rcabench_execption" / datapack_path.name,
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
    save_parquet(index_df, path=meta_folder / "index.parquet")
    save_parquet(labels_df, path=meta_folder / "labels.parquet")
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
    dataset = "rcabench_execption"
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
