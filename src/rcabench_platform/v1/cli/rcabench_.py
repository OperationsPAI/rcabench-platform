from ..utils.serde import save_json
from ..clients.k8s import download_kube_info
from ..spec.data import TEMP
from ..clients.rcabench_ import CustomRCABenchSDK, get_rcabench_sdk
from ..logging import logger, timeit

from pprint import pprint
from pathlib import Path

import typer
import rcabench.model.injection

app = typer.Typer()


@app.command()
@timeit()
def query_dataset(name: str):
    sdk = CustomRCABenchSDK()

    output = sdk.query_dataset(name=name)
    pprint(output)


@app.command()
@timeit()
def query_injection(name: str):
    sdk = CustomRCABenchSDK()

    output = sdk.query_injection(name=name)
    pprint(output)


@app.command()
@timeit()
def kube_info(save_path: Path = TEMP / "kube_info.json"):
    kube_info = download_kube_info(ns="ts1")
    save_json(kube_info.to_dict(), path=save_path)


@app.command()
@timeit()
def list_injections(page_num: int = 1, page_size: int = 10):
    sdk = get_rcabench_sdk()

    output = sdk.injection.list(page_num=page_num, page_size=page_size)
    assert isinstance(output, rcabench.model.injection.ListResult)
    pprint(output.model_dump())
