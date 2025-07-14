from ..config import get_config
from ..utils.serde import save_json
from ..clients.k8s import download_kube_info
from ..clients.rcabench_ import RcabenchSdkHelper
from ..logging import logger, timeit

from pprint import pprint
from pathlib import Path

import typer

app = typer.Typer()


@app.command()
@timeit()
def kube_info(save_path: Path | None = None):
    kube_info = download_kube_info(ns="ts1")

    if save_path is None:
        config = get_config()
        save_path = config.temp / "kube_info.json"

    save_json(kube_info.to_dict(), path=save_path)


@app.command()
@timeit()
def query_injection(name: str):
    sdk = RcabenchSdkHelper()
    resp = sdk.get_injection_details(dataset_name=name)
    pprint(resp.model_dump())


@app.command()
@timeit()
def list_injections():
    sdk = RcabenchSdkHelper()
    output = sdk.list_injections()

    items = []
    for item in output:
        items.append(item.model_dump())

    pprint(items)
