from ..logging import logger

import typer

app = typer.Typer()


@app.command()
def test() -> None:
    logger.info("Hello from rcabench-platform!")
