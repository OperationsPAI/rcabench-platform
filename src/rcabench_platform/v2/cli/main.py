from ..algorithms.spec import global_algorithm_registry
from ..utils.env import getenv_bool
from ..logging import logger, timeit

import multiprocessing

from tqdm.auto import tqdm
import typer


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.callback()
def _():
    logger.remove()
    logger.add(
        lambda msg: tqdm.write(msg, end=""),
        colorize=getenv_bool("LOGURU_COLORIZE", default=True),
        enqueue=True,
        context=multiprocessing.get_context("spawn"),
    )


def main(*, enable_builtin_algorithms: bool = True) -> None:
    from . import self_, tools, rcabench_, sdg, eval

    app.add_typer(self_.app, name="self")
    app.add_typer(tools.app, name="tools")
    app.add_typer(rcabench_.app, name="rcabench")
    app.add_typer(sdg.app, name="sdg")
    app.add_typer(eval.app, name="eval")

    if enable_builtin_algorithms:
        register_builtin_algorithms()

    app()


def register_builtin_algorithms():
    from ..algorithms.random_ import Random

    from ..algorithms.traceback.A7 import TraceBackA7
    from ..algorithms.traceback.A8 import TraceBackA8

    from ..algorithms.rcaeval.baro import Baro
    from ..algorithms.rcaeval.nsigma import NSigma

    getters = {
        "random": Random,
        "traceback-A7": TraceBackA7,
        "traceback-A8": TraceBackA8,
        "baro": Baro,
        "nsigma": NSigma,
    }

    registry = global_algorithm_registry()
    for name, getter in getters.items():
        registry[name] = getter
