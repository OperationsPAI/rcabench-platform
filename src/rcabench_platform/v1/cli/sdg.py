from ..graphs.sdg.dump import dump_place_indicators
from ..graphs.sdg.neo4j import export_sdg_to_neo4j
from ..spec.data import DATA_ROOT, TEMP_SDG
from ..utils.serde import save_parquet, save_pickle
from ..graphs.sdg.build_ import build_sdg
from ..logging import logger, timeit

from pathlib import Path
import resource

import typer

app = typer.Typer()


@app.command()
@timeit()
def build(
    dataset: str,
    datapack: str,
    neo4j: bool = True,
) -> None:
    sdg = build_sdg(dataset, datapack, DATA_ROOT / dataset / datapack)

    maxrss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    maxrss_mib = maxrss_kib / 1024
    logger.info(f"Peak memory usage: {maxrss_mib:.3f} MiB")

    sdg_pkl_path = TEMP_SDG / "sdg.pkl"
    save_pickle(sdg, path=sdg_pkl_path)

    sdg_pkl_size = sdg_pkl_path.stat().st_size / 1024 / 1024
    logger.info(f"SDG pickle size: {sdg_pkl_size:.3f} MiB")

    df = dump_place_indicators(sdg)
    save_parquet(df, path=TEMP_SDG / "indicators.parquet")

    if neo4j:
        export_sdg_to_neo4j(sdg)
