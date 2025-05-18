from ..utils.serde import save_json
from ..clients.k8s import download_kube_info
from ..folders import TEMP
from ..clients.rcabench_ import CustomRCABenchSDK
from ..logging import logger, timeit

from pprint import pprint
from pathlib import Path

import typer

app = typer.Typer()


@app.command()
@timeit()
def query_dataset(name: str):
    sdk = CustomRCABenchSDK()

    output = sdk.query_dataset(name=name)
    pprint(output)


@app.command()
@timeit()
def kube_info(save_path: Path = TEMP / "kube_info.json"):
    kube_info = download_kube_info(ns="ts1")
    save_json(kube_info.to_dict(), path=save_path)
