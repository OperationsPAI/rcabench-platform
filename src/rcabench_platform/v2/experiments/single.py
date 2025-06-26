from .spec import get_output_folder

from ..datasets.spec import get_datapack_labels, get_datapack_folder
from ..algorithms.spec import AlgorithmArgs, global_algorithm_registry
from ..logging import logger, timeit
from ..utils.serde import save_parquet
from ..utils.fs import running_mark
from ..evaluation.ranking import calc_all_perf

from pathlib import Path
import dataclasses
import traceback
import time

import polars as pl


@timeit(log_level="INFO")
def run_single(algorithm: str, dataset: str, datapack: str, *, clear: bool = False, skip_finished: bool = True):
    alg = global_algorithm_registry()[algorithm]()

    input_folder = get_datapack_folder(dataset, datapack)
    output_folder = get_output_folder(dataset, datapack, algorithm)

    with running_mark(output_folder, clear=clear):
        finished = output_folder / ".finished"
        if skip_finished and finished.exists():
            logger.debug(f"skipping {output_folder}")
            return

        try:
            t0 = time.time()
            answers = (alg)(
                AlgorithmArgs(
                    dataset=dataset,
                    datapack=datapack,
                    input_folder=input_folder,
                    output_folder=output_folder,
                )
            )
            t1 = time.time()
            exc = None
            runtime = t1 - t0
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error in {algorithm} for {dataset}/{datapack}: {repr(e)}")
            answers = []
            exc = e
            runtime = None

    answers.sort(key=lambda x: x.rank)
    for no, ans in enumerate(answers, start=1):
        assert ans.rank == no, f"Answer {no} rank {ans.rank} not in order"

    logger.debug(f"len(answers)={len(answers)}")

    answers = [dataclasses.asdict(ans) for ans in answers]
    if len(answers) == 0:
        answers.append({"level": None, "name": None, "rank": 1})

    labels_set = {(label.level, label.name) for label in get_datapack_labels(dataset, datapack)}
    hits = [(ans["level"], ans["name"]) in labels_set for ans in answers]

    if exc is not None:
        exception_type = type(exc).__name__
        exception_message = "".join(traceback.format_exception(None, exc, tb=exc.__traceback__))
    else:
        exception_type = None
        exception_message = None

    output_df = pl.DataFrame(
        answers,
        schema={"level": pl.String, "name": pl.String, "rank": pl.UInt32},
    ).with_columns(
        pl.lit(algorithm).alias("algorithm"),
        pl.lit(dataset).alias("dataset"),
        pl.lit(datapack).alias("datapack"),
        pl.Series(hits, dtype=pl.Boolean).alias("hit"),
        pl.lit(runtime, dtype=pl.Float64).alias("runtime.seconds"),
        pl.lit(exception_type, dtype=pl.String).alias("exception.type"),
        pl.lit(exception_message, dtype=pl.String).alias("exception.message"),
    )

    if output_df["hit"].any():
        for row in output_df.filter(pl.col("hit")).iter_rows(named=True):
            logger.debug(f"hit: {row}")
    else:
        logger.debug("No hit")

    save_parquet(output_df, path=output_folder / "output.parquet")

    perf_df = calc_all_perf(output_df, agg_level="datapack")
    save_parquet(perf_df, path=output_folder / "perf.parquet")

    finished.touch()
