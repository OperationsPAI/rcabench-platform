from .single import run_single

from ..utils.fmap import fmap_processpool
from ..datasets.spec import get_datapack_list
from ..algorithms.spec import global_algorithm_registry
from ..logging import logger, timeit

import multiprocessing
import functools
import random
import time
import sys
import os


@timeit(log_level="INFO")
def run_batch(
    algorithms: list[str],
    datasets: list[str],
    *,
    sample: int | None = None,
    clear: bool = False,
    skip_finished: bool = True,
    use_cpus: int | None = None,
):
    registry = global_algorithm_registry()
    for algorithm in algorithms:
        assert algorithm in registry

    logger.debug(f"algorithms=`{algorithms}`")

    for dataset in datasets:
        datapacks = get_datapack_list(dataset)

        if sample is not None:
            assert sample > 0
            k = min(sample, len(datapacks))
            datapacks = random.sample(datapacks, k)

        for algorithm in algorithms:
            alg = registry[algorithm]()
            alg_cpu_count = alg.needs_cpu_count()

            if alg_cpu_count is None:
                parallel = 0
            else:
                assert alg_cpu_count > 0
                usable_cpu_count = use_cpus or max(multiprocessing.cpu_count() - 4, 0)
                parallel = usable_cpu_count // alg_cpu_count

            del alg

            tasks = []
            for datapack in datapacks:
                tasks.append(
                    functools.partial(
                        run_single,
                        algorithm,
                        dataset,
                        datapack,
                        clear=clear,
                        skip_finished=skip_finished,
                    )
                )

            t0 = time.time()
            fmap_processpool(tasks, parallel=parallel, cpu_limit_each=alg_cpu_count)
            t1 = time.time()

            total_walltime = t1 - t0
            avg_walltime = total_walltime / len(tasks)

            logger.debug(f"Total   walltime: {total_walltime:.3f} seconds")
            logger.debug(f"Average walltime: {avg_walltime:.3f} seconds")

            logger.debug(f"Finished running algorithm `{algorithm}` on dataset `{dataset}`")

            sys.stdout.flush()
