#!/usr/bin/env -S uv run -s
from rcabench_platform.v1.cli.main import app, logger
from rcabench_platform.v1.logging import timeit
from rcabench_platform.v1.spec.cloud import Storage, MinioStorage, HuggingFaceStorage

from typing import Literal
from pathlib import Path

from huggingface_hub import HfApi
import minio


def get_minio_client() -> minio.Minio:
    return minio.Minio(
        "10.10.10.38:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )


def get_hf_client():
    return HfApi()


def get_storage(*, name: str, direction: Literal["upload", "download"]) -> Storage:
    if direction == "upload":
        local_root = Path("data/rcabench_platform_datasets")
    elif direction == "download":
        local_root = Path("temp/cloud")
        local_root.mkdir(parents=True, exist_ok=True)
    else:
        raise ValueError(f"Invalid direction: {direction}")

    if name == "minio":
        return MinioStorage(
            local_root=local_root,
            minio_client=get_minio_client(),
            bucket="temp",
            object_root="cloud",
            concurrent_download=16,
            concurrent_upload=8,
        )
    elif name == "huggingface":
        return HuggingFaceStorage(
            local_root=local_root,
            hf_client=get_hf_client(),
            repo_id="Nugine/rcabench-platform-private-test",
            concurrent_download=8,
            concurrent_upload=4,
        )
    else:
        raise ValueError(f"Invalid storage name: {name}")


@app.command()
@timeit()
def upload(storage_name: str, dataset: str):
    storage = get_storage(name=storage_name, direction="upload")

    storage.upload_dataset(dataset)


@app.command()
@timeit()
def download(storage_name: str, dataset: str):
    storage = get_storage(name=storage_name, direction="download")

    storage.download_dataset(dataset)


if __name__ == "__main__":
    app()
