from ..utils.env import getenv_bool
from ..logging import logger

import multiprocessing

from tqdm.auto import tqdm
import typer


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.callback()
def main():
    logger.remove()
    logger.add(
        lambda msg: tqdm.write(msg, end=""),
        format="<lvl>[{time}][{elapsed}][{level}][{file}:{line}][{function}]</lvl>: {message}",
        colorize=getenv_bool("LOGURU_COLORIZE", default=True),
        enqueue=True,
        context=multiprocessing.get_context("spawn"),
    )


@app.command()
def self_test() -> None:
    logger.info("Hello from rcabench-platform!")
