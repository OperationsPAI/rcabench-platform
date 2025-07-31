import json
from pathlib import Path
from typing import Annotated, Any

import polars as pl
import typer
from rcabench.openapi import (
    AlgorithmsApi,
    DatasetsApi,
    DtoAlgorithmExecutionRequest,
    DtoAlgorithmItem,
    DtoBatchAlgorithmExecutionRequest,
    DtoDatasetV2SearchReq,
    DtoInjectionV2SearchReq,
    InjectionsApi,
)

from ..clients.k8s import download_kube_info
from ..clients.rcabench_ import RCABenchClient, get_rcabench_openapi_client
from ..config import get_config
from ..logging import logger, timeit
from ..utils.dataframe import print_dataframe
from ..utils.serde import save_json

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
def query_injection(name: str, page: int = 1, size: int = 5):
    with RCABenchClient() as client:
        api = InjectionsApi(client)
        resp = api.api_v2_injections_search_post(
            search=DtoInjectionV2SearchReq(
                search=name,
                page=page,
                size=size,
            )
        )
    assert resp.data is not None

    ans = resp.data.model_dump()
    print_json(ans)


@app.command()
@timeit()
def list_injections():
    with RCABenchClient() as client:
        api = InjectionsApi(client)
        resp = api.api_v2_injections_get()
        assert resp.data is not None
    assert resp.data.items is not None
    ans = [item.model_dump() for item in resp.data.items]
    print_json(ans)


@app.command()
@timeit()
def list_datasets():
    with RCABenchClient() as client:
        api = DatasetsApi(client)
        resp = api.api_v2_datasets_search_post(search=DtoDatasetV2SearchReq(search=""))
        assert resp.data is not None
    assert resp.data.items is not None

    data = []
    for item in resp.data.items:
        data.append({"ID": item.id, "Name": item.name, "Version": item.version, "Status": item.status})

    df = pl.DataFrame(data)
    print_dataframe(df)


@app.command()
@timeit()
def list_algorithms():
    with RCABenchClient() as client:
        api = AlgorithmsApi(client)
        resp = api.api_v2_algorithms_get()
        assert resp.data is not None

        assert resp.data.items is not None
        ans = [item.model_dump() for item in resp.data.items]
        print_json(ans)


@app.command()
@timeit()
def submit_execution(
    algorithms: Annotated[list[str], typer.Option("-a", "--algorithm")],
    datapacks: Annotated[list[str] | None, typer.Option("-d", "--datapack")] = None,
    datasets: Annotated[str | None, typer.Option("-ds", "--dataset")] = None,
    dataset_versions: Annotated[str | None, typer.Option("-dsv", "--dataset-version")] = None,
    envs: Annotated[list[str] | None, typer.Option("--env")] = None,
):
    assert algorithms, "At least one algorithm must be specified."
    assert datapacks or datasets, "At least one datapack or dataset must be specified."
    assert not (datapacks and datasets), "Cannot specify both datapacks and datasets."

    dataset_list = [datasets.strip()] if datasets and datasets.strip() else []
    dataset_version_list = [dataset_versions.strip()] if dataset_versions and dataset_versions.strip() else []

    if datasets and dataset_versions and len(dataset_list) != len(dataset_version_list):
        raise ValueError("The number of datasets and dataset versions must be the same.")

    env_vars: dict[str, str] = {}
    if envs is not None:
        for env in envs:
            if "=" not in env:
                raise ValueError(f"Invalid environment variable format: `{env}`. Expected 'key=value'.")
            key, value = env.split("=", 1)
            env_vars[key] = value

    payloads: list[DtoAlgorithmExecutionRequest] = []
    with RCABenchClient() as client:
        api = AlgorithmsApi(client)
        for algorithm in algorithms:
            if dataset_list:
                for dataset, dataset_version in zip(dataset_list, dataset_version_list):
                    payload = DtoAlgorithmExecutionRequest(
                        algorithm=DtoAlgorithmItem(name=algorithm),
                        dataset=dataset,
                        dataset_version=dataset_version,
                        env_vars=env_vars,
                        project_name="pair_diagnosis",
                    )

                    payloads.append(payload)

            if datapacks:
                for datapack in datapacks:
                    payload = DtoAlgorithmExecutionRequest(
                        algorithm=DtoAlgorithmItem(name=algorithm),
                        datapack=datapack,
                        env_vars=env_vars,
                        project_name="pair_diagnosis",
                    )
                    payloads.append(payload)

            resp = api.api_v2_algorithms_execute_post(
                request=DtoBatchAlgorithmExecutionRequest(
                    executions=payloads,
                    project_name="pair_diagnosis",
                )
            )
            assert resp.data is not None

            executions = resp.data.executions
            assert executions is not None
            data = []
            for i, execution in enumerate(executions):
                row = {
                    "Index": i + 1,
                    "Datapack": execution.datapack_id,
                    "Dataset": execution.dataset_id,
                    "Algorithm": execution.algorithm_id,
                    "Status": execution.status,
                    "Task ID": execution.task_id,
                    "Trace ID": execution.trace_id,
                }

                data.append(row)

            df = pl.DataFrame(data)
            print_dataframe(df)
