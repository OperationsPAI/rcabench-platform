from ..algorithms.traceback.a7 import TraceBackA7
from ..algorithms.random_ import Random

from ..evaluation.ranking import calc_all_perf, calc_all_perf_by_datapack_attr
from ..spec.data import (
    DATA_ROOT,
    META_ROOT,
    OUTPUT,
    OUTPUT_META,
    dataset_index_path,
    find_ground_truths,
    get_datapack_list,
)
from ..spec.algorithm import Algorithm, AlgorithmArgs
from ..logging import logger, timeit

from ..utils.dataframe import print_dataframe
from ..utils.fmap import fmap_processpool
from ..utils.serde import save_parquet
from ..utils.fs import running_mark

from collections.abc import Callable
from dataclasses import asdict
import multiprocessing
import functools
import traceback
import random
import time
import re
import os

from tqdm.auto import tqdm
import polars as pl
import typer

app = typer.Typer()

ALGORITHMS: dict[str, Callable[..., Algorithm]] = {
    "random": Random,
    "traceback-A7": TraceBackA7,
}


def build_algorithm(alg: str) -> Algorithm:
    return ALGORITHMS[alg]()


@app.command()
@timeit(log_level="INFO")
def run(alg: str, dataset: str, datapack: str, *, clear: bool = False) -> None:
    algorithm = build_algorithm(alg)

    input_folder = DATA_ROOT / dataset / datapack
    output_folder = OUTPUT / dataset / datapack / alg

    with running_mark(output_folder, clear=clear):
        finished = output_folder / ".finished"
        if finished.exists():
            logger.debug(f"skipping {output_folder}")
            return

        try:
            t0 = time.time()
            answers = (algorithm)(
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
            logger.error(f"Error in {alg} for {dataset}/{datapack}: {type(e)} {repr(e)}")
            answers = []
            exc = e
            runtime = None

    answers.sort(key=lambda x: x.rank)
    for no, ans in enumerate(answers, start=1):
        assert ans.rank == no, f"Answer {no} rank {ans.rank} not in order"

    logger.debug(f"len(answers)={len(answers)}")

    answers = [asdict(ans) for ans in answers]
    if len(answers) == 0:
        answers.append({"level": None, "name": None, "rank": 1})

    ground_truths = find_ground_truths(dataset, datapack)
    hits = [(ans["level"], ans["name"]) in ground_truths for ans in answers]

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
        pl.lit(alg).alias("algorithm"),
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


def get_usable_cpu_count() -> int:
    usable_cpu_count = multiprocessing.cpu_count()

    if (max_cpu_env := os.environ.get("RCABENCH_CPU_COUNT")) is not None:
        usable_cpu_count = min(int(max_cpu_env), usable_cpu_count)
    else:
        usable_cpu_count //= 2

    return usable_cpu_count


@app.command()
@timeit(log_level="INFO")
def run_all(
    alg_pattern: str,
    dataset: str,
    *,
    sample: int | None = None,
    clear: bool = False,
):
    algorithms = []
    for alg_name in ALGORITHMS.keys():
        if re.match(alg_pattern, alg_name):
            algorithms.append(alg_name)

    logger.debug(f"algorithms=`{algorithms}`")

    datapacks = get_datapack_list(dataset)

    if sample is not None:
        assert sample > 0
        k = min(sample, len(datapacks))
        datapacks = random.sample(datapacks, k)

    for alg in algorithms:
        algorithm = build_algorithm(alg)
        alg_cpu_count = algorithm.needs_cpu_count()

        if alg_cpu_count is None:
            parallel = 0
        else:
            assert alg_cpu_count > 0
            usable_cpu_count = get_usable_cpu_count()
            parallel = usable_cpu_count // alg_cpu_count

            if parallel:
                os.environ["POLARS_MAX_THREADS"] = str(alg_cpu_count)

        del algorithm

        tasks = []
        for datapack, _ in datapacks:
            tasks.append(functools.partial(run, alg, dataset, datapack, clear=clear))

        t0 = time.time()
        fmap_processpool(tasks, parallel=parallel)
        t1 = time.time()

        total_walltime = t1 - t0
        avg_walltime = total_walltime / len(tasks)

        logger.debug(f"Total   walltime: {total_walltime:.3f} seconds")
        logger.debug(f"Average walltime: {avg_walltime:.3f} seconds")

        logger.debug(f"Finished running algorithm `{alg}` on dataset `{dataset}`")


@app.command()
@timeit(log_level="INFO")
def perf(dataset: str, warn_missing: bool = False):
    index_path = dataset_index_path(dataset)
    index_df = pl.read_parquet(index_path)

    datapacks: list[str] = index_df["datapack"].unique().to_list()
    algorithms = sorted(ALGORITHMS.keys())

    items = [(datapack, alg) for datapack in datapacks for alg in algorithms]

    output_paths = [OUTPUT / dataset / datapack / alg / "output.parquet" for datapack, alg in items]

    logger.debug("loading output files")

    df_list: list[pl.DataFrame] = []
    for path in tqdm(output_paths):
        if path.exists():
            df = pl.read_parquet(path)
            df_list.append(df)
        elif warn_missing:
            logger.warning(f"missing output file: {path}")

    output_df = pl.concat(df_list)
    save_parquet(output_df, path=OUTPUT_META / dataset / "output.parquet")

    if dataset.startswith("rcabench"):
        attributes_df_path = META_ROOT / dataset / "attributes.parquet"
        if attributes_df_path.exists():
            attr_col = "injection.fault_type"
            attr_df = pl.read_parquet(attributes_df_path, columns=["datapack", attr_col])

            perf_df = calc_all_perf_by_datapack_attr(
                output_df.join(attr_df, on="datapack", how="left"),
                dataset,
                attr_col,
            )
            save_parquet(perf_df, path=OUTPUT_META / dataset / "fault_types.perf.parquet")

    perf_df = calc_all_perf(output_df, agg_level="datapack")
    save_parquet(perf_df, path=OUTPUT_META / dataset / "datapack.perf.parquet")

    perf_df = calc_all_perf(output_df, agg_level="dataset")
    save_parquet(perf_df, path=OUTPUT_META / dataset / "dataset.perf.parquet")

    print_dataframe(
        perf_df.select(
            "dataset",
            "algorithm",
            "total",
            "error",
            "runtime.seconds:avg",
            "MRR",
            "AC@1.count",
            "AC@3.count",
            "AC@5.count",
            "AC@1",
            "AC@3",
            "AC@5",
        )
    )


@app.command()
@timeit(log_level="INFO")
def exp_rcaeval(clear: bool = False):
    datasets = [
        "rcaeval_re2_tt",
        "rcaeval_re2_ob",
    ]

    for dataset in datasets:
        run_all(".*", dataset, clear=clear)

    for dataset in datasets:
        perf(dataset)
