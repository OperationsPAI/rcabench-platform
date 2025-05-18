from ..logging import get_real_logger, set_real_logger, logger

from collections.abc import Callable, Sequence
from typing import Literal
import multiprocessing
import multiprocessing.pool
import traceback
import sys

from tqdm.auto import tqdm


def _fmap[R](
    mode: Literal["threadpool", "processpool"],
    tasks: Sequence[Callable[[], R]],
    *,
    parallel: int | None = None,
    ignore_exceptions: bool = False,
) -> list[R]:
    assert isinstance(tasks, list)

    if parallel is None or parallel > 1:
        num_workers = parallel or multiprocessing.cpu_count()
        num_workers = min(num_workers, len(tasks))
    else:
        num_workers = 1

    if num_workers > 1:
        if mode == "threadpool":
            pool = multiprocessing.pool.ThreadPool(
                processes=num_workers,
            )
        elif mode == "processpool":
            pool = multiprocessing.get_context("spawn").Pool(
                processes=num_workers,
                initializer=set_real_logger,
                initargs=(get_real_logger(),),
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")

        with pool:
            asyncs = [pool.apply_async(task) for task in tasks]
            results = []
            for result in tqdm(asyncs, desc=f"fmap_{mode}"):
                if ignore_exceptions:
                    try:
                        results.append(result.get())
                    except Exception as e:
                        traceback.print_exc()
                        logger.opt(depth=1).error(f"Error in task: {e}")
                else:
                    results.append(result.get())
                sys.stdout.flush()
    else:
        results = []
        for task in tqdm(tasks, desc=f"fmap_{mode}"):
            result = task()
            results.append(result)
            sys.stdout.flush()

    return results


def fmap_threadpool[R](
    tasks: Sequence[Callable[[], R]],
    *,
    parallel: int | None = None,
    ignore_exceptions: bool = False,
) -> list[R]:
    return _fmap("threadpool", tasks, parallel=parallel, ignore_exceptions=ignore_exceptions)


def fmap_processpool[R](
    tasks: Sequence[Callable[[], R]],
    *,
    parallel: int | None = None,
    ignore_exceptions: bool = False,
) -> list[R]:
    return _fmap("processpool", tasks, parallel=parallel, ignore_exceptions=ignore_exceptions)
