#!/usr/bin/env -S uv run -s
import os
import shutil
import subprocess
from pathlib import Path

from rcabench_platform.v2.cli.main import app, timeit

CONTEXTS: dict[str, Path] = {
    "rcabench-platform": Path.cwd(),
    "clickhouse_dataset": Path.cwd() / "docker/clickhouse_dataset",
    "detector": Path.cwd() / "docker/detector",
}

IMAGE_PREFIX = "registry.example.org/library"


def sh(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def sh_out(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def get_commit_hash() -> str:
    return sh_out(["git", "rev-parse", "--short", "HEAD"])


def assert_clean():
    out = sh_out(["git", "status", "--porcelain"])
    if out:
        raise RuntimeError("Working directory is not clean")


def get_project_version() -> str:
    return sh_out(["uv", "version", "--short"])


@app.command()
@timeit()
def build(image_name: str):
    assert_clean()

    directory = CONTEXTS[image_name]
    os.chdir(directory)

    commit_hash = get_commit_hash()
    version = get_project_version()

    tmp = None
    if image_name == "rcabench-platform":
        tmp = Path(".docker-build")
        tmp.mkdir(exist_ok=True)

        sh(["uv", "version", "0.0.0"])
        shutil.copyfile("pyproject.toml", tmp / "pyproject.toml")
        shutil.copyfile("uv.lock", tmp / "uv.lock")

    http_proxy = os.getenv("HTTP_PROXY", "")
    https_proxy = os.getenv("HTTPS_PROXY", "")

    cmd = ["docker", "build"]
    cmd.extend(["--network=host"])

    if http_proxy:
        cmd.extend(["--build-arg", f"HTTP_PROXY={http_proxy}"])

    if https_proxy:
        cmd.extend(["--build-arg", f"HTTPS_PROXY={https_proxy}"])

    image_full = f"{IMAGE_PREFIX}/{image_name}:{commit_hash}"
    cmd.extend(["-t", image_full])
    cmd.extend(["-f", "Dockerfile", "."])

    try:
        sh(cmd)
    finally:
        if tmp:
            shutil.rmtree(tmp)
        sh(["uv", "version", version])

    image_latest = f"{IMAGE_PREFIX}/{image_name}:latest"
    sh(["docker", "tag", image_full, image_latest])


@app.command()
@timeit()
def push(image_name: str):
    directory = CONTEXTS[image_name]
    os.chdir(directory)

    commit_hash = get_commit_hash()

    image_full = f"{IMAGE_PREFIX}/{image_name}:{commit_hash}"
    sh(["docker", "push", image_full])

    image_latest = f"{IMAGE_PREFIX}/{image_name}:latest"
    sh(["docker", "push", image_latest])


@app.command()
@timeit()
def update_all():
    for image_name in CONTEXTS:
        build(image_name)

    for image_name in CONTEXTS:
        push(image_name)


if __name__ == "__main__":
    app()
