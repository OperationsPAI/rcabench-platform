from ..clients.rcabench_ import CustomRCABenchSDK
from ..logging import logger, timeit

from pprint import pprint

import typer

app = typer.Typer()


@app.command()
@timeit()
def query_dataset(name: str):
    sdk = CustomRCABenchSDK()

    output = sdk.query_dataset(name=name)
    pprint(output)
