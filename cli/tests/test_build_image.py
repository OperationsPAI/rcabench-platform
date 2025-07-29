#!/usr/bin/env -S uv run -s
import os
from collections.abc import Generator
from pathlib import Path

import dotenv
from rcabench.const import EventType, TaskStatus
from rcabench.model.error import ModelHTTPError
from rcabench.model.trace import AlgorithmItem, ExecutionOptions, InfoPayload, StreamEvent
from rcabench.openapi.api import AlgorithmApi, ContainerApi, DatasetApi
from rcabench.openapi.api_client import ApiClient, Configuration
from rcabench.openapi.models.dto_algorithm_item import DtoAlgorithmItem
from rcabench.openapi.models.dto_dataset_build_payload import DtoDatasetBuildPayload
from rcabench.openapi.models.dto_execution_payload import DtoExecutionPayload
from rcabench.openapi.models.dto_generic_response_dto_submit_resp import DtoGenericResponseDtoSubmitResp
from rcabench.rcabench import RCABenchSDK

from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.config import get_config
from rcabench_platform.v2.logging import timeit

ENV_PATH = ".env"
TOKEN_KEY = "GITHUB_TOKEN"

TEST_DATASET = "ts3-ts-order-service-return-qkcshv"
TEST_NAMESPACE = "ts"
TEST_PRE_DURATION = 4


def get_algorithm_item(host: str, response: DtoGenericResponseDtoSubmitResp) -> None:
    res = trace_execution(host=host, response=response, timeout=600)
    for event in res:
        if isinstance(event, ModelHTTPError):
            logger.error(f"Error in event stream: {event.detail}")
        else:
            if event.event_name == EventType.EventImageBuildSucceed:
                assert event.payload is not None
                item = AlgorithmItem.model_validate_json(event.payload)
                logger.info(f"Build succeeded: {item}")
            elif event.event_name == EventType.EventTaskStatusUpdate:
                assert event.payload is not None
                item = InfoPayload.model_validate_json(event.payload)
                if item.status == TaskStatus.ERROR:
                    logger.error(f"Build failed: {item.message}")


def get_execution_payload(host: str, response: DtoGenericResponseDtoSubmitResp) -> None:
    res = trace_execution(host=host, response=response, timeout=600)
    for event in res:
        if isinstance(event, ModelHTTPError):
            logger.error(f"Error in event stream: {event.detail}")
        else:
            if event.event_name == EventType.EventAlgoRunSucceed:
                assert event.payload is not None
                options = ExecutionOptions.model_validate_json(event.payload)
                logger.info(f"Execution succeeded: {options}")
            elif event.event_name == EventType.EventTaskStatusUpdate:
                assert event.payload is not None
                item = InfoPayload.model_validate_json(event.payload)
                if item.status == TaskStatus.ERROR:
                    logger.error(f"Execution failed: {item.message}")


def get_configuration(host: str) -> Configuration:
    configuration = Configuration(host=host)
    configuration.datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    return configuration


def set_token() -> None:
    env_file = Path(ENV_PATH)
    if env_file.exists():
        dotenv.load_dotenv(env_file, override=True)
        logger.info(f"Loaded environment variables from {env_file}")
    else:
        logger.warning(f"{env_file} not found")


def trace_execution(
    host: str, response: DtoGenericResponseDtoSubmitResp, timeout: float | None = 600
) -> Generator[StreamEvent | ModelHTTPError, None, None]:
    assert response.data is not None
    logger.info(f"Submit response: {response.model_dump()}")

    traces = response.data.traces
    assert traces is not None

    trace_ids = [trace.trace_id for trace in traces]
    assert trace_ids[0] is not None

    sdk = RCABenchSDK(host)
    return sdk.trace.stream_trace_events(trace_id=trace_ids[0], timeout=timeout)


@app.command()
@timeit()
def local(
    env: str = "prod",
    container_type: str = "algorithm",
    name: str = "traceback-test",
    image: str = "10.10.10.240/library/rca-algo-traceback-test-local-file",
    tag: str = "latest",
    command: str = "bash /entrypoint.sh",
    env_vars: str | None = None,
    filename: str = "traceback.zip",
    context_dir: str = ".",
    dockerfile_path: str = "Dockerfile",
    force_rebuild: bool = False,
):
    """
    Build and upload local file to get container

    Args:
        env: Environment mode (prod or dev or debug)
        container_type: Type of container (algorithm or benchmark)
        name: Container name
        image: Docker image name
        tag: Image tag
        command: Command to run in the container
        env_vars: Environment variable names, comma-separated (e.g., "VAR1,VAR2,VAR3")
        filename: Source code filename
        context_dir: Context directory for Docker build
        dockerfile_path: Path to Dockerfile in source code
        force_rebuild: Whether to force rebuild
    """

    config = get_config(env_mode=env)

    file_dir = config.temp
    file_path = file_dir / filename
    if not file_path.exists():
        logger.error(f"File {file_path} does not exist. Please provide a valid file.")
        raise FileNotFoundError(f"{file_path} does not exist.")

    with open(file_path, "rb") as f:
        file_content = f.read()

    configuration = get_configuration(host=config.base_url)
    with ApiClient(configuration=configuration) as client:
        api = ContainerApi(api_client=client)
        resp = api.api_v1_containers_post(
            type=container_type,
            name=name,
            image=image,
            tag=tag,
            source_type="file",
            command=command,
            env_vars=env_vars.split(",") if env_vars else None,
            file=(filename, file_content),
            context_dir=context_dir,
            dockerfile_path=dockerfile_path,
            force_rebuild=force_rebuild,
        )

    assert resp is not None
    get_algorithm_item(host=config.base_url, response=resp)


@app.command()
@timeit()
def github(
    env: str = "prod",
    container_type: str = "algorithm",
    name: str = "traceback-test",
    image: str = "10.10.10.240/library/rca-algo-traceback-test-github",
    tag: str = "latest",
    command: str = "bash /entrypoint.sh",
    env_vars: str | None = None,
    repo: str = "LGU-SE-Internal/rca-algo-contrib",
    branch: str = "main",
    path: str = "algorithms/traceback",
    context_dir: str = ".",
    dockerfile_path: str = "Dockerfile",
    force_rebuild: bool = False,
):
    """
    Build and upload GitHub repository to get container

    Args:
        container_type: Type of container (algorithm or benchmark)
        env: Environment mode (prod or dev or debug)
        name: Container name
        image: Docker image name
        tag: Image tag
        command: Command to run in the container
        env_vars: Environment variable names, comma-separated (e.g., "VAR1,VAR2,VAR3")
        repo: GitHub repository (owner/repo)
        branch: Git branch name
        path: Sub-directory path in repository
        context_dir: Context directory for Docker build
        dockerfile_path: Path to Dockerfile in repository
        force_rebuild: Whether to force rebuild
    """

    set_token()
    config = get_config(env_mode=env)

    token = os.getenv(TOKEN_KEY)
    if token is None:
        logger.error(f"Environment variable {TOKEN_KEY} not set. Please set it to your GitHub token.")
        raise ValueError(f"Environment variable {TOKEN_KEY} not set.")

    configuration = get_configuration(host=config.base_url)
    with ApiClient(configuration=configuration) as client:
        api = ContainerApi(api_client=client)
        resp = api.api_v1_containers_post(
            type=container_type,
            name=name,
            image=image,
            tag=tag,
            command=command,
            env_vars=env_vars.split(",") if env_vars else None,
            source_type="github",
            github_token=token,
            github_repo=repo,
            github_branch=branch,
            github_path=path,
            context_dir=context_dir,
            dockerfile_path=dockerfile_path,
            force_rebuild=force_rebuild,
        )

    get_algorithm_item(host=config.base_url, response=resp)


if __name__ == "__main__":
    app()
