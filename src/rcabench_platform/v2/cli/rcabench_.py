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


@app.command()
@timeit()
def execute_algorithm(
    algorithm_name: str,
    dataset_name: str,
    image: str | None = None,
    tag: str | None = None,
):
    """Execute an algorithm on a dataset.
    
    Args:
        algorithm_name: Name of the algorithm to execute
        dataset_name: Name of the dataset/datapack to process
        image: Optional container image name
        tag: Optional container image tag
    """
    sdk = RcabenchSdkHelper()
    
    try:
        resp = sdk.execute_algorithm(
            algorithm_name=algorithm_name,
            dataset_name=dataset_name,
            image=image,
            tag=tag
        )
        
        print(f"Algorithm execution submitted successfully!")
        print(f"Response code: {resp.code}")
        print(f"Message: {resp.message}")
        
        if resp.data and resp.data.traces:
            print(f"Execution traces:")
            for trace in resp.data.traces:
                print(f"  - Trace ID: {trace.trace_id}")
                print(f"    Head Task ID: {trace.head_task_id}")
                print(f"    Index: {trace.index}")
        
        # Determine success/failure based on response code
        if resp.code == 200:
            print("Status: SUCCESS")
        else:
            print("Status: FAILURE")
            
    except Exception as e:
        print(f"Algorithm execution failed: {str(e)}")
        print("Status: FAILURE")
