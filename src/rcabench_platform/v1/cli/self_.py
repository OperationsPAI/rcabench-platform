from ..logging import logger, timeit

import typer

app = typer.Typer()


@app.command()
@timeit()
def test() -> None:
    logger.info("Hello from rcabench-platform!")
