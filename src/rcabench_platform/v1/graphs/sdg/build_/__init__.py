from .rcaeval import build_sdg_from_rcaeval

from pathlib import Path


def build_sdg(dataset: str, datapack: str, input_folder: Path):
    if dataset.startswith("rcaeval"):
        return build_sdg_from_rcaeval(dataset, datapack, input_folder)
    else:
        raise NotImplementedError
