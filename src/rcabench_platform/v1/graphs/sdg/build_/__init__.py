from .rcaeval import build_sdg_from_rcaeval


def build_sdg(dataset: str, datapack: str):
    if dataset.startswith("rcaeval"):
        return build_sdg_from_rcaeval(dataset, datapack)
    else:
        raise NotImplementedError
