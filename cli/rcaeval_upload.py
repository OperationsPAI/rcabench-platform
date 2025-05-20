#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.clients.minio_ import get_minio_client
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.utils.fmap import fmap_threadpool

from pathlib import Path
import functools

import minio


def upload(src_file: Path, dst_file: Path):
    client = get_minio_client()

    bucket_name = "rcaeval-dataset"
    object_name = str(dst_file)

    try:
        client.stat_object(bucket_name, object_name)
        logger.info(f"Object `{bucket_name}/{object_name}` already exists. Skipping upload.")
        return
    except minio.error.S3Error as e:
        if e.code != "NoSuchKey":
            raise

    client.fput_object(
        bucket_name=bucket_name,
        object_name=str(dst_file),
        file_path=str(src_file),
    )

    logger.info(f"File {src_file} uploaded to {bucket_name}/{object_name}.")


@app.command()
@timeit()
def run(nfs_path: Path):
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
                        dst_file = file.relative_to(nfs_path)
                        tasks.append(functools.partial(upload, file, dst_file))

    fmap_threadpool(tasks, parallel=8)


if __name__ == "__main__":
    app()
