#!/usr/bin/env -S uv run -s
from pathlib import Path

from dotenv import load_dotenv

from rcabench_platform.v2.analysis.detector_visualization import batch_visualization
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.datasets.rcabench import valid

load_dotenv()


@app.command()
def manual_vis_detector(datapacks: list[Path], skip_existing: bool = False) -> None:
    batch_visualization(datapacks, skip_existing)


@app.command()
def auto_vis_detector(skip_existing: bool = True) -> None:
    datapack_path = Path("data") / "rcabench_dataset"
    if not datapack_path.exists():
        logger.error(f"Datapack directory not found: {datapack_path}")
        return

    valid_datapacks = []
    for p in datapack_path.iterdir():
        if p.is_dir() and valid(datapack_path / p.name):
            valid_datapacks.append(p)

    batch_visualization(valid_datapacks, skip_existing)
