import dataclasses
import time
import traceback
from pathlib import Path

import polars as pl
from rcabench.openapi import (
    AlgorithmApi,
    AlgorithmsApi,
    DtoAlgorithmSearchRequest,
    DtoGranularityResultEnhancedRequest,
    DtoGranularityResultItem,
    DtoInjectionV2SearchReq,
    InjectionsApi,
)

from ..algorithms.spec import AlgorithmArgs, global_algorithm_registry
from ..clients.rcabench_ import RCABenchClient
from ..config import get_config
from ..datasets.spec import get_datapack_folder, get_datapack_labels
from ..evaluation.ranking import calc_all_perf
from ..logging import logger, timeit
from ..utils.fs import running_mark
from ..utils.serde import save_parquet
from .spec import get_output_folder


@timeit(log_level="INFO")
def run_single(
    algorithm: str,
    dataset: str,
    datapack: str,
    *,
    clear: bool = False,
    skip_finished: bool = True,
    submit_result: bool = False,
):
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

    if submit_result:
        assert runtime is not None
        with RCABenchClient(base_url=get_config().base_url) as client:
            algorithms_api = AlgorithmsApi(client)
            injections_api = InjectionsApi(client)
            injections = injections_api.api_v2_injections_search_post(
                search=DtoInjectionV2SearchReq(
                    search=f"{datapack}",
                    page=1,
                    size=1,
                ),
            )
            assert injections.data is not None and injections.data.items is not None
            datapack_id = injections.data.items[0].id

            algorithms = algorithms_api.api_v2_algorithms_search_post(
                request=DtoAlgorithmSearchRequest(
                    name=algorithm,
                    page=1,
                    size=10,
                ),
            )
            assert algorithms.code is not None
            if algorithms.code > 210:
                logger.error(
                    f"Error in algorithms search API call for {algorithm}: {algorithms.code}, {algorithms.message}"
                )
                return
            assert algorithms.data is not None
            assert algorithms.data.items is not None

            # Find algorithm by name
            algorithm_id = None
            for algo in algorithms.data.items:
                if algo.name == algorithm:
                    algorithm_id = algo.id
                    break

            if algorithm_id is None:
                logger.warning(f"Algorithm '{algorithm}' not found in available algorithms")
                return
            assert algorithm_id is not None

            resp = algorithms_api.api_v2_algorithms_algorithm_id_results_post(
                algorithm_id=algorithm_id,
                request=DtoGranularityResultEnhancedRequest(
                    duration=runtime,
                    datapack_id=datapack_id,
                    results=[
                        DtoGranularityResultItem(
                            confidence=0,
                            level=ans["level"],
                            rank=ans["rank"],
                            result=ans["name"],
                        )
                        for ans in answers
                    ],
                ),
            )
            logger.info(f"result of submitting {algorithm} for {dataset}/{datapack}: {resp.code}, {resp.message}")
