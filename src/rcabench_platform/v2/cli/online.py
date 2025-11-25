import os
from pathlib import Path
from typing import Annotated, TypedDict

import polars as pl
import tomli
import typer
from rcabench.openapi import (
    ContainersApi,
    ContainerSpec,
    DatasetRef,
    DatasetsApi,
    ExecutionsApi,
    ExecutionSpec,
    InjectionsApi,
    LabelItem,
    ParameterSpec,
    SearchInjectionReq,
    SubmitExecutionReq,
    TracesApi,
)

from rcabench_platform.v2.analysis.data_prepare import get_execution_item

from ..clients.k8s import download_kube_info
from ..clients.rcabench_ import get_rcabench_client
from ..config import get_config
from ..logging import logger, timeit
from ..metrics.algo_metrics import get_algorithms_metrics_across_datasets
from ..utils.dataframe import print_dataframe
from ..utils.serde import save_json

app = typer.Typer(pretty_exceptions_enable=False)


@app.command()
@timeit()
def kube_info(namespace: str = "ts1", save_path: Path | None = None):
    kube_info = download_kube_info(ns=namespace)

    if save_path is None:
        config = get_config()
        save_path = config.temp / "kube_info.json"

    ans = kube_info.to_dict()
    save_json(ans, path=save_path)

    # Convert dict to DataFrame for display
    df = pl.DataFrame([ans])
    print_dataframe(df)


@app.command()
@timeit()
def query_injection(name: str, base_url: str | None = None):
    client = get_rcabench_client(base_url=base_url)
    api = InjectionsApi(client)
    resp = api.search_injections(
        search=SearchInjectionReq(
            name_pattern=name,
        )
    )
    assert resp.code is not None and resp.code < 300 and resp.data is not None, "No injection found"

    ans = resp.data.model_dump()
    # Convert dict to DataFrame for display
    df = pl.DataFrame([ans])
    print_dataframe(df)


@app.command()
@timeit()
def list_injections(base_url: str | None = None):
    client = get_rcabench_client(base_url=base_url)
    api = InjectionsApi(client)
    resp = api.list_injections()
    assert resp.code is not None and resp.code < 300 and resp.data is not None and resp.data.items is not None

    ans = [item.model_dump() for item in resp.data.items]
    # Convert list of dicts to DataFrame for display
    df = pl.DataFrame(ans)
    print_dataframe(df)


@app.command()
@timeit()
def list_datasets(base_url: str | None = None):
    client = get_rcabench_client(base_url=base_url)
    api = DatasetsApi(client)
    resp = api.list_datasets()
    assert resp.code is not None and resp.code < 300 and resp.data is not None and resp.data.items is not None

    data = []
    for item in resp.data.items:
        data.append({"ID": item.id, "Name": item.name, "Status": item.status})

    df = pl.DataFrame(data)
    print_dataframe(df)


@app.command()
@timeit()
def get_dataset(id: int,base_url: str | None = None):
    client = get_rcabench_client(base_url=base_url)
    api = DatasetsApi(client)
    resp = api.get_dataset_by_id(dataset_id=id)
    assert resp.data is not None

    # Return dataset versions if available
    if resp.data.versions:
        return [f"{resp.data.name}@{v.name}" for v in resp.data.versions]
    return []


@app.command()
@timeit()
def list_algorithms(base_url: str | None = None):
    client = get_rcabench_client(base_url=base_url)
    api = ContainersApi(client)
    resp = api.list_containers()
    assert resp.data is not None

    assert resp.data.items is not None
    ans = [item.model_dump() for item in resp.data.items]
    # Convert list of dicts to DataFrame for display
    df = pl.DataFrame(ans)
    print_dataframe(df)


class AlgoSpec(TypedDict):
    name: str
    image: str | None
    tag: str | None


def parse_algorithm_spec(algo_string: str,base_url: str | None = None) -> AlgoSpec:
    """
    Parse algorithm specification string.

    Supports two formats:
    1. Algorithm name: "algorithm_name" or "algorithm_name:tag"
    2. Docker image: "registry/image:tag" (contains '/' character)

    Args:
        algo_string: Algorithm specification string

    Returns:
        AlgoSpec with name, image, and tag fields
    """
    algo_string = algo_string.strip()

    def get_default_tag(algorithm_name: str) -> str | None:
        client = get_rcabench_client(base_url=base_url)
        try:
                from rcabench.openapi import ContainerType

                api = ContainersApi(client)
                # List container versions for the algorithm and get the latest
                resp = api.list_containers(type=ContainerType.Algorithm)
                if resp.data and resp.data.items:
                    for container in resp.data.items:
                        if container.name == algorithm_name and container.id is not None:
                            # Get container versions
                            versions_resp = api.list_container_versions(container_id=container.id)
                            if versions_resp.data and versions_resp.data.items:
                                # Return the first version (assumed to be latest)
                                latest = versions_resp.data.items[0]
                                logger.info(f"found latest tag: {algorithm_name}:{latest.name}")
                                return latest.name
                return None
        except Exception:
                return None

    is_docker_image = "/" in algo_string
    has_tag = ":" in algo_string

    if has_tag:
        base_part, tag = algo_string.rsplit(":", 1)
        name = base_part.split("/")[-1] if is_docker_image else base_part
        image = base_part if is_docker_image else None
        return AlgoSpec(name=name.strip(), image=image.strip() if image else None, tag=tag.strip())

    # No tag specified, need to get default tag
    name = algo_string.split("/")[-1] if is_docker_image else algo_string
    image = algo_string if is_docker_image else None
    default_tag = get_default_tag(name.strip())

    return AlgoSpec(name=name.strip(), image=image.strip() if image else None, tag=default_tag)


@app.command()
@timeit()
def submit_execution(
    algorithms: Annotated[
        list[str],
        typer.Option(
            "-a",
            "--algorithm",
            help="Algorithm specification: 'name' or 'name:tag' for algorithm name, "
            "'registry/image:tag' for docker image",
        ),
    ],
    project: Annotated[str | None, typer.Option("-p", "--project")] = None,
    datapacks: Annotated[list[str] | None, typer.Option("-d", "--datapack")] = None,
    dataset: Annotated[str | None, typer.Option("-ds", "--dataset")] = None,
    dataset_version: Annotated[str | None, typer.Option("-dsv", "--dataset-version")] = None,
    envs: Annotated[list[str] | None, typer.Option("--env")] = None,
    base_url: Annotated[str | None, typer.Option("--base-url")] = None,
    tag: Annotated[str | None, typer.Option("--tag")] = None,
):
    assert algorithms, "At least one algorithm must be specified."
    assert datapacks or dataset, "At least one datapack or dataset must be specified."
    assert not (datapacks and dataset), "Cannot specify both datapacks and datasets."
    assert project, "Project name must be specified."
    assert tag, "Tag must be specified."

    parsed_algorithms = [parse_algorithm_spec(algo) for algo in algorithms]

    dataset_list = [dataset.strip()] if dataset and dataset.strip() else []
    dataset_version_list = [dataset_version.strip()] if dataset_version and dataset_version.strip() else []

    if dataset and dataset_version and len(dataset_list) != len(dataset_version_list):
        raise ValueError("The number of datasets and dataset versions must be the same.")

    env_vars: dict[str, str] = {}
    if envs is not None:
        for env in envs:
            if "=" not in env:
                raise ValueError(f"Invalid environment variable format: `{env}`. Expected 'key=value'.")
            key, value = env.split("=", 1)
            env_vars[key] = value

    # Convert env_vars dict to list of ParameterSpec
    env_params = [ParameterSpec(key=k, value={"value": v}) for k, v in env_vars.items()] if env_vars else None

    with RCABenchClient(base_url=base_url) as client:
        api = ExecutionsApi(client)
        for algorithm_spec in parsed_algorithms:
            specs: list[ExecutionSpec] = []
            if dataset_list:
                for dataset, dataset_version in zip(dataset_list, dataset_version_list):
                    spec = ExecutionSpec(
                        algorithm=ContainerSpec(
                            name=algorithm_spec["name"],
                            version=algorithm_spec["tag"],
                            env_vars=env_params,
                        ),
                        dataset=DatasetRef(
                            name=dataset,
                            version=dataset_version,
                        ),
                    )
                    specs.append(spec)

            if datapacks:
                for datapack in datapacks:
                    spec = ExecutionSpec(
                        algorithm=ContainerSpec(
                            name=algorithm_spec["name"],
                            version=algorithm_spec["tag"],
                            env_vars=env_params,
                        ),
                        datapack=datapack,
                    )
                    specs.append(spec)

            labels = [LabelItem(key="tag", value=tag)] if tag else None
            resp = api.run_algorithm(
                request=SubmitExecutionReq(
                    specs=specs,
                    project_name=project,
                    labels=labels,
                )
            )
            assert resp.data is not None

            executions = resp.data.items
            assert executions is not None
            data = []
            for i, execution in enumerate(executions):
                row = {
                    "Index": i + 1,
                    "Datapack": execution.datapack_id,
                    "Dataset": execution.dataset_id,
                    "Algorithm": execution.algorithm_id,
                    "Status": "submitted",
                    "Task ID": execution.task_id,
                    "Trace ID": execution.trace_id,
                }

                data.append(row)

            df = pl.DataFrame(data)
            print_dataframe(df)


def check_required_files(algo_folder: Path) -> bool:
    """Check if required files exist in algorithm folder"""
    required_files = ["info.toml", "Dockerfile", "entrypoint.sh"]

    for file_name in required_files:
        file_path = algo_folder / file_name
        if not file_path.exists():
            logger.warning(f"{algo_folder} missing file: {file_name}")
            return False

    return True


def parse_toml_config(info_file: Path) -> tuple[str, dict[str, str], str, str]:
    """Parse info.toml file to extract name and env_vars"""
    algorithm_name = info_file.parent.name
    env_vars = {}
    tag = "latest"
    command = "bash /entrypoint.sh"

    if info_file.exists():
        try:
            with open(info_file, "rb") as f:
                config = tomli.load(f)

            if "name" in config:
                algorithm_name = config["name"]
            if "env_vars" in config:
                env_vars = config["env_vars"]
            if "tag" in config:
                tag = config["tag"]
            if "command" in config:
                command = config["command"]

        except Exception as e:
            logger.warning(f"Failed to parse TOML file {info_file}: {e}")

    return algorithm_name, env_vars, tag, command


@app.command()
@timeit()
def upload_algorithm_harbor(
    algo_folder: Annotated[Path, typer.Argument(help="Algorithm folder path")],
    base_url: Annotated[str | None, typer.Option("--base-url")] = None,
):
    """
    Upload algorithm record with pre-built image from Harbor registry

    🐳 HARBOR MODE: Use pre-built images from Harbor registry
    - No file upload required
    - Assumes image is already built and pushed to Harbor
    - Backend uses existing Harbor image
    - Requires: Image must exist in Harbor registry
    """
    logger.info(f"🐳 Using HARBOR MODE for algorithm: {algo_folder}")

    # Check required files
    if not check_required_files(algo_folder):
        logger.error(f"Missing required files in {algo_folder}")
        return False

    try:
        # Read info.toml to get algorithm name and env_vars
        info_file = algo_folder / "info.toml"
        algorithm_name, env_vars, tag, command = parse_toml_config(info_file)

        logger.info(f"Uploading algorithm: {algorithm_name}")
        if env_vars:
            logger.info(f"Environment variables: {env_vars}")

        with RCABenchClient(base_url=base_url) as api_client:
            from rcabench.openapi import ContainerType, CreateContainerReq, CreateContainerVersionReq

            api = ContainersApi(api_client=api_client)

            # Create container with version
            image_ref = f"10.10.10.240/library/rca-algo-{algorithm_name}:{tag}"
            resp = api.create_container(
                request=CreateContainerReq(
                    name=algorithm_name,
                    type=ContainerType.Algorithm,
                    is_public=False,
                    version=CreateContainerVersionReq(
                        name=tag,
                        image_ref=image_ref,
                        command=command,
                    ),
                )
            )

        logger.info(f"Response: {resp}")

        if resp.code == 200:
            logger.info(f"✅ Successfully uploaded algorithm: {algorithm_name}")
            return True
        else:
            logger.error(f"❌ Upload failed: {algorithm_name}")
            return False

    except Exception as e:
        logger.error(f"❌ Algorithm upload failed {algo_folder}: {e}")
        return False


@app.command()
def trace(trace_id: str, base_url: str | None = None, timeout: int = 600):
    base_url = base_url or os.getenv("RCABENCH_BASE_URL")
    assert base_url is not None, "base_url or RCABENCH_BASE_URL is not set"

    with RCABenchClient(base_url=base_url) as client:
        api = TracesApi(client)
        # Note: stream_trace_events returns None in the new API
        # The SSE streaming needs to be handled differently
        # Using _request_timeout for timeout
        logger.warning("Trace streaming API has changed - this function may need to be reimplemented")
        api.stream_trace_events(trace_id=trace_id, _request_timeout=timeout)


@app.command()
def cross_dataset_metrics(
    algorithms: Annotated[list[str], typer.Option("-a", "--algorithm")],
    datasets: Annotated[list[str], typer.Option("-d", "--dataset")],
    dataset_versions: Annotated[list[str], typer.Option("-dv", "--dataset-version")],
    tag: Annotated[str | None, typer.Option("--tag")] = None,
    base_url: Annotated[str | None, typer.Option("--base-url")] = None,
    level: Annotated[str | None, typer.Option("-l", "--level")] = None,
):
    metrics = get_algorithms_metrics_across_datasets(algorithms, datasets, dataset_versions, tag, base_url, level)

    df = pl.DataFrame(metrics)
    print_dataframe(df)


@app.command(name="guda")
def get_unevaluated_datapack_algo(
    algorithms: Annotated[list[str], typer.Argument(help="List of algorithm names")],
    dataset_id: Annotated[int | None, typer.Option("--dataset-id", "-d", help="Dataset ID")] = None,
):
    assert dataset_id is not None
    _, run_status_map = get_execution_item(algorithms, dataset_id)

    data = []
    for datapack_name, algorithm_name in run_status_map:
        data.append({"algorithm": algorithm_name, "datapack": datapack_name})

    df = pl.DataFrame(data)
    print_dataframe(df)
    return run_status_map


@app.command(name="submit-unevaluated")
@timeit()
def submit_unevaluated_execution(
    algorithms: Annotated[
        list[str], typer.Argument(help="List of algorithm names, only support algoname:tag. e.g., baro:acfdb44")
    ],
    tag: Annotated[str, typer.Option("--tag", help="Tag for the execution")],
    project: Annotated[str | None, typer.Option("-p", "--project", help="Project name")] = None,
    dataset_id: Annotated[int | None, typer.Option("--dataset-id", "-d", help="Dataset ID")] = None,
    envs: Annotated[list[str] | None, typer.Option("--env")] = None,
    base_url: Annotated[str | None, typer.Option("--base-url")] = None,
):
    if project is None:
        project = "pair_diagnosis"
    logger.info("Fetching unevaluated datapack-algorithm pairs...")
    unevaluated_pairs = get_unevaluated_datapack_algo(
        [i.split(":")[0] for i in algorithms],
        dataset_id,
    )

    if not unevaluated_pairs:
        logger.info("No unevaluated datapack-algorithm pairs found")
        return

    logger.info(f"Found {len(unevaluated_pairs)} unevaluated datapack-algorithm pairs")

    # Parse environment variables
    env_vars: dict[str, str] = {}
    if envs is not None:
        for env in envs:
            if "=" not in env:
                raise ValueError(f"Invalid environment variable format: `{env}`. Expected 'key=value'.")
            key, value = env.split("=", 1)
            env_vars[key] = value

    # Convert env_vars dict to list of ParameterSpec
    env_params = [ParameterSpec(key=k, value={"value": v}) for k, v in env_vars.items()] if env_vars else None

    # Build algorithm specifications
    parsed_algorithms = [parse_algorithm_spec(algo) for algo in algorithms]

    # Build execution request specs
    specs: list[ExecutionSpec] = []

    # Group unevaluated pairs by algorithm
    algo_datapack_map: dict[str, list[str]] = {}
    for algorithm_name, datapack_name in unevaluated_pairs:
        if algorithm_name not in algo_datapack_map:
            algo_datapack_map[algorithm_name] = []
        algo_datapack_map[algorithm_name].append(datapack_name)

    with RCABenchClient(base_url=base_url) as client:
        api = ExecutionsApi(client)

        for algorithm_spec in parsed_algorithms:
            algorithm_name = algorithm_spec["name"]

            if algorithm_name not in algo_datapack_map:
                logger.warning(f"Algorithm {algorithm_name} has no unevaluated datapacks")
                continue

            for datapack in algo_datapack_map[algorithm_name]:
                spec = ExecutionSpec(
                    algorithm=ContainerSpec(
                        name=algorithm_spec["name"],
                        version=algorithm_spec["tag"],
                        env_vars=env_params,
                    ),
                    datapack=datapack,
                )
                specs.append(spec)

        if not specs:
            logger.warning("No valid execution specs")
            return

        logger.info(f"Submitting {len(specs)} execution tasks...")

        labels = [LabelItem(key="tag", value=tag)] if tag else None
        resp = api.run_algorithm(
            request=SubmitExecutionReq(
                specs=specs,
                project_name=project,
                labels=labels,
            )
        )
        assert resp.data is not None

        executions = resp.data.items
        assert executions is not None

        data = []
        for i, execution in enumerate(executions):
            row = {
                "Index": i + 1,
                "Datapack": execution.datapack_id,
                "Dataset": execution.dataset_id,
                "Algorithm": execution.algorithm_id,
                "Task ID": execution.task_id,
                "Trace ID": execution.trace_id,
            }
            data.append(row)

        df = pl.DataFrame(data)
        print_dataframe(df)

        logger.info(f"✅ Successfully submitted {len(executions)} execution tasks")


def main():
    app()
