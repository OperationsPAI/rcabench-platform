from ..utils.env import getenv_bool
from ..logging import logger

import multiprocessing
import importlib

from tqdm.auto import tqdm
import typer


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.callback()
def main():
    logger.remove()
    logger.add(
        lambda msg: tqdm.write(msg, end=""),
        colorize=getenv_bool("LOGURU_COLORIZE", default=True),
        enqueue=True,
        context=multiprocessing.get_context("spawn"),
    )


def with_subcommands() -> typer.Typer:
    module_map = {
        "self_": "self",
        "rcabench_": "rcabench",
        "tools": "tools",
        "sdg": "sdg",
        "eval": "eval",
    }

    for module_name, subcommand in module_map.items():
        module = importlib.import_module(f".{module_name}", package=__package__)
        sub_app = getattr(module, "app")
        assert isinstance(sub_app, typer.Typer)
        app.add_typer(sub_app, name=subcommand)

    return app
