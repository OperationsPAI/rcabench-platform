from ..config import get_config
from ..utils.serde import save_json
from ..clients.k8s import download_kube_info
from ..clients.rcabench_ import get_rcabench_openapi_client
from ..logging import logger, timeit

from pathlib import Path
from typing import Any
import json

from rcabench.openapi import InjectionApi, AlgorithmApi

import typer

app = typer.Typer()


def print_json(data: Any):
    print(json.dumps(data, indent=4, ensure_ascii=False), flush=True)


@app.command()
@timeit()
def kube_info(namespace: str = "ts1", save_path: Path | None = None):
    kube_info = download_kube_info(ns=namespace)

    if save_path is None:
        config = get_config()
        save_path = config.temp / "kube_info.json"

    ans = kube_info.to_dict()
    save_json(ans, path=save_path)

    print_json(ans)


@app.command()
@timeit()
def query_injection(name: str):
    api = InjectionApi(get_rcabench_openapi_client())
    resp = api.api_v1_injections_query_get(name=name)
    assert resp.data is not None

    ans = resp.data.model_dump()
    print_json(ans)


@app.command()
@timeit()
def list_injections():
    api = InjectionApi(get_rcabench_openapi_client())
    resp = api.api_v1_injections_get()
    assert resp.data is not None

    ans = [item.model_dump() for item in resp.data]
    print_json(ans)


@app.command()
@timeit()
def list_algorithms():
    api = AlgorithmApi(get_rcabench_openapi_client())
    resp = api.api_v1_algorithms_get()
    assert resp.data is not None

    ans = [item.model_dump() for item in resp.data]
    print_json(ans)
