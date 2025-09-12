#!/usr/bin/env -S uv run -s
import tarfile
import zipfile
from pathlib import Path

from rcabench.openapi import InjectionsApi

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.config import get_config
from rcabench_platform.v2.datasets.rcabench import valid


def compress_all(
    tags: list[str] | None = None,
    output_dir: Path = Path("output"),
    format: str = "tar.gz",
    output_filename: str | None = None,
) -> Path:
    if tags is None:
        with RCABenchClient(base_url=get_config().base_url) as client:
            injection_api = InjectionsApi(client)
            resp = injection_api.api_v2_injections_get(tags=None, page=1, size=10000)
            assert resp.code is not None and resp.code < 300 and resp.data is not None and resp.data.items is not None
            logger.info(f"found {len(resp.data.items)} injections")
            datapack_names = [
                item.injection_name
                for item in resp.data.items
                if (
                    item.injection_name is not None  # and valid(Path("data") / "rcabench_dataset" / item.injection_name
                )
            ]
    else:
        datapack_names = []
        for tag in tags:
            with RCABenchClient(base_url=get_config().base_url) as client:
                injection_api = InjectionsApi(client)
                resp = injection_api.api_v2_injections_get(tags=[tag], page=1, size=10000)
                assert (
                    resp.code is not None and resp.code < 300 and resp.data is not None and resp.data.items is not None
                )
                logger.info(f"found {len(resp.data.items)} injections for tag {tag}")
                validated = [item.injection_name for item in resp.data.items if item.injection_name is not None]
                logger.info(f"found {len(validated)} valid injections for tag {tag}")
                datapack_names.extend(validated)

    logger.info(f"Found {len(datapack_names)} datapacks to compress")

    data_base_dir = Path("data/rcabench_dataset")
    output_dir.mkdir(parents=True, exist_ok=True)

    if output_filename is None:
        if tags:
            tag_str = "-".join(tags)
            output_filename = f"rcabench-{tag_str}"
        else:
            output_filename = "rcabench-all"

    if format == "tar.gz":
        output_file = output_dir / f"{output_filename}.tar.gz"
    elif format == "tar.xz":
        output_file = output_dir / f"{output_filename}.tar.xz"
    elif format == "zip":
        output_file = output_dir / f"{output_filename}.zip"
    else:
        logger.error(f"Unsupported format: {format}")
        raise ValueError(f"Unsupported format: {format}")

    logger.info(f"Creating single compressed file: {output_file}")

    valid_datapacks = []
    for datapack_name in datapack_names:
        datapack_dir = data_base_dir / datapack_name
        converted_dir = datapack_dir / "converted"

        if not converted_dir.exists():
            logger.warning(f"converted directory not found: {converted_dir}")
            continue

        valid_datapacks.append((datapack_name, converted_dir))

    logger.info(f"Found {len(valid_datapacks)} valid datapacks to include")

    try:
        if format in ["tar.gz", "tar.xz"]:
            mode = "w:gz" if format == "tar.gz" else "w:xz"
            with tarfile.open(output_file, mode) as tar:
                for datapack_name, converted_dir in valid_datapacks:
                    logger.info(f"Adding {datapack_name} to archive...")
                    tar.add(converted_dir, arcname=datapack_name)
        elif format == "zip":
            with zipfile.ZipFile(output_file, "w", zipfile.ZIP_DEFLATED) as zf:
                for datapack_name, converted_dir in valid_datapacks:
                    logger.info(f"Adding {datapack_name} to archive...")
                    for file_path in converted_dir.rglob("*"):
                        if file_path.is_file():
                            arcname = datapack_name / file_path.relative_to(converted_dir)
                            zf.write(file_path, arcname)

        logger.info(f"Successfully created compressed file: {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Failed to create compressed file: {e}")
        raise


if __name__ == "__main__":
    output_file = compress_all(tags=["absolute_anomaly"])
    print(f"Created compressed file: {output_file}")
