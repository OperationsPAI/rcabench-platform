#!/usr/bin/env -S uv run -s
import shutil
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.utils.fmap import fmap_threadpool

from pathlib import Path
import functools
import tempfile
import subprocess

from tqdm.auto import tqdm
import polars as pl


TMP = Path("/dev/shm/convert_legacy")


@app.command()
@timeit()
def run():
    TMP.mkdir(exist_ok=True)

    root = Path("data") / "rcabench_dataset"
    datapacks = [f.name for f in root.iterdir() if f.is_dir()]

    tasks = []
    for datapack in tqdm(datapacks):
        data_tar_gz = root / datapack / "data.tar.gz"
        if not data_tar_gz.exists():
            continue

        # if (root / datapack / "abnormal_traces.parquet").exists():
        #     continue

        tasks.append(functools.partial(convert, root / datapack))

    fmap_threadpool(tasks, parallel=16)


@timeit()
def convert(folder: Path):
    with tempfile.TemporaryDirectory(dir=TMP) as tmpdir:
        tmpdir = Path(tmpdir) / folder.name
        tmpdir.mkdir()

        data_tar_gz = folder / "data.tar.gz"
        subprocess.run(["tar", "-xzf", str(data_tar_gz), "-C", str(tmpdir)], check=True)

        for file in tmpdir.iterdir():
            if file.is_symlink():
                continue

            ext = file.suffix
            if ext == ".csv":
                dst = tmpdir / file.with_suffix(".parquet").name
                convert_csv_to_parquet(file, dst)
                shutil.move(dst, folder / dst.name)
            else:
                shutil.move(file, folder / file.name)


@timeit()
def convert_csv_to_parquet(src: Path, dst: Path):
    lf = pl.scan_csv(src, infer_schema_length=50000)
    schema = lf.collect_schema()
    columns = schema.names()

    time_cols = ["Timestamp", "TimeUnix"]
    for col in time_cols:
        if col not in columns:
            continue
        lf = lf.with_columns(pl.col(col).str.to_datetime().dt.replace_time_zone("Asia/Shanghai"))

    col = "SpanAttributes"
    if col in columns:
        lf = lf.with_columns(pl.col(col).str.replace_all("'", r'"', literal=True))

    lf.sink_parquet(dst)


if __name__ == "__main__":
    app()
