#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.clients.minio_ import get_minio_client
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.utils.fmap import fmap_threadpool

from pathlib import Path
import functools

import minio


@timeit()
def upload_dataset(source_dir: Path):
    client = get_minio_client()

    dataset_name = source_dir.name
    logger.info(f"Uploading dataset {dataset_name} to MinIO...")

    bucket_name = "rcabench-dataset"

    for file in source_dir.iterdir():
        if file.is_dir():
            continue

        object_name = f"{dataset_name}/{file.name}"

        try:
            client.stat_object(bucket_name, object_name)
            logger.info(f"Object `{bucket_name}/{object_name}` already exists. Skipping upload.")
            continue
        except minio.error.S3Error as e:
            if e.code != "NoSuchKey":
                raise

        client.fput_object(
            bucket_name="rcabench-dataset",
            object_name=f"{dataset_name}/{file.name}",
            file_path=str(file),
        )

    logger.info(f"Dataset {dataset_name} uploaded successfully.")


@app.command()
@timeit()
def run(nfs_path: Path):
    tasks = []
    for item in nfs_path.iterdir():
        if item.is_dir():
            tasks.append(functools.partial(upload_dataset, item))
        else:
            logger.warning(f"Skipping non-directory {item}")

    fmap_threadpool(tasks, parallel=8)


if __name__ == "__main__":
    app()
