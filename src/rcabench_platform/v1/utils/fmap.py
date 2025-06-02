from ..logging import get_real_logger, set_real_logger, logger, timeit

from collections.abc import Callable, Sequence
from typing import Literal
import multiprocessing
import multiprocessing.pool
import traceback
import time

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

    logger_ = logger.opt(depth=2)

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
        finished = [False] * len(asyncs)
        index_results: list[tuple[int, R]] = []
        exception_count = 0

        with tqdm(total=len(asyncs), desc=f"fmap_{mode}") as pbar:
            while not all(finished):
                for i, async_ in enumerate(asyncs):
                    if finished[i]:
                        continue
                    if not async_.ready():
                        continue
                    try:
                        result = async_.get(timeout=0.1)
                        finished[i] = True
                        index_results.append((i, result))
                        pbar.update(1)
                    except multiprocessing.TimeoutError:
                        continue
                    except Exception as e:
                        exception_count += 1
                        finished[i] = True
                        pbar.update(1)
                        if ignore_exceptions:
                            traceback.print_exc()
                            logger_.error(f"Exception in task: {e}")
                        else:
                            raise e
                pbar.update(0)
                time.sleep(1)

    index_results.sort(key=lambda x: x[0])
    results = [result for _, result in index_results]

    if exception_count > 0:
        logger_.warning(f"fmap_{mode} completed with {exception_count} exceptions.")

    logger_.debug(f"fmap_{mode} completed with {len(results)} results in {len(tasks)} tasks.")

    return results


@timeit(log_args=False)
def fmap_threadpool[R](
    tasks: Sequence[Callable[[], R]],
    *,
    parallel: int | None = None,
    ignore_exceptions: bool = False,
) -> list[R]:
    return _fmap("threadpool", tasks, parallel=parallel, ignore_exceptions=ignore_exceptions)


@timeit(log_args=False)
def fmap_processpool[R](
    tasks: Sequence[Callable[[], R]],
    *,
    parallel: int | None = None,
    ignore_exceptions: bool = False,
) -> list[R]:
    return _fmap("processpool", tasks, parallel=parallel, ignore_exceptions=ignore_exceptions)
