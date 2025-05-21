#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.utils.fmap import fmap_threadpool

from pathlib import Path
import functools
import shutil


@timeit()
def upload(src_file: Path, dst_file: Path):
    dst_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src_file, dst_file)


@app.command()
@timeit()
def run(nfs_path: Path, jfs_path: Path):
    tasks = []
    for dataset in nfs_path.iterdir():
        if not dataset.is_dir():
            continue
        for service in dataset.iterdir():
            if not service.is_dir():
                continue
            for num in service.iterdir():
                if not num.is_dir():
                    continue
                for file in num.iterdir():
                    if file.is_file():
                        dst_file = jfs_path / file.relative_to(nfs_path)
                        tasks.append(functools.partial(upload, file, dst_file))

    fmap_threadpool(tasks, parallel=8)


if __name__ == "__main__":
    app()
