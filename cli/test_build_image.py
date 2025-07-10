#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.logging import timeit
from rcabench_platform.v2.config import get_config

from collections.abc import Generator
from pathlib import Path
import json
import os

from rcabench.openapi.api import AlgorithmApi
from rcabench.const import EventType
from rcabench.model.error import ModelHTTPError
from rcabench.model.trace import AlgorithmItem, StreamEvent
from rcabench.openapi.api_client import ApiClient, Configuration
from rcabench.openapi.exceptions import BadRequestException, NotFoundException, ServiceException
from rcabench.openapi.models.dto_generic_response_dto_submit_resp import DtoGenericResponseDtoSubmitResp
from rcabench.rcabench import RCABenchSDK

import dotenv

ENV_PATH = ".env"
TOKEN_KEY = "GITHUB_TOKEN"


def get_algorithm_item(host: str, response: DtoGenericResponseDtoSubmitResp) -> None:
    assert response.data is not None
    logger.info(f"Build image response: {response.model_dump()}")

    traces = response.data.traces
    assert traces is not None

    trace_ids = [trace.trace_id for trace in traces]
    assert trace_ids[0] is not None

    res = trace_execution(host=host, trace_id=trace_ids[0])
    for event in res:
        if isinstance(event, ModelHTTPError):
            logger.error(f"Error in event stream: {event.detail}")
        else:
            if event.event_name == EventType.EventImageBuildSucceed:
                assert event.payload is not None
                payload = json.loads(event.payload)
                item = AlgorithmItem.model_validate(payload)
                logger.info(f"Build succeeded: {item}")


def get_configuration(host: str) -> Configuration:
    configuration = Configuration(host=host)
    configuration.datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    return configuration


def load_env_file() -> None:
    env_file = Path(ENV_PATH)
    if env_file.exists():
        dotenv.load_dotenv(env_file, override=True)
        logger.info(f"Loaded environment variables from {env_file}")
    else:
        logger.warning(f"{env_file} not found")


def trace_execution(
    host: str, trace_id: str, timeout: float | None = 600
) -> Generator[StreamEvent | ModelHTTPError, None, None]:
    sdk = RCABenchSDK(host)
    return sdk.trace.stream_trace_events(trace_id, timeout=timeout)


@app.command()
@timeit()
def local(
    algorithm: str = "traceback",
    filename: str = "traceback.zip",
    image: str = "10.10.10.240/library/rca-algo-traceback-local-file",
    tag: str = "latest",
    force_rebuild: bool = False,
):
    """
    Build and upload local file algorithm image

    Args:
        filename: Source code filename
        algorithm: Algorithm name
        image: Docker image name
        tag: Image tag
        force_rebuild: Whether to force rebuild
    """

    load_env_file()
    config = get_config()

    file_dir = config.data if config.env_mode == "prod" else config.temp
    file_path = file_dir / filename
    if not file_path.exists():
        logger.error(f"File {file_path} does not exist. Please provide a valid file.")
        return

    with open(file_path, "rb") as f:
        file_content = f.read()

    resp = None
    try:
        configuration = get_configuration(host=config.base_url)
        with ApiClient(configuration=configuration) as client:
            api = AlgorithmApi(api_client=client)
            resp = api.api_v1_algorithms_build_post(
                algorithm=algorithm,
                image=image,
                tag=tag,
                source_type="file",
                file=(filename, file_content),
                force_rebuild=force_rebuild,
            )
    except (BadRequestException, NotFoundException, ServiceException) as e:
        logger.error(f"Failed to build algorithm '{algorithm}': {e}")
        return

    if resp is not None:
        get_algorithm_item(host=config.base_url, response=resp)


@app.command()
@timeit()
def github(
    algorithm: str = "traceback",
    image: str = "10.10.10.240/library/rca-algo-traceback-github",
    tag: str = "latest",
    repo: str = "LGU-SE-Internal/rca-algo-contrib",
    branch: str = "main",
    path: str = "algorithms/traceback",
    force_rebuild: bool = False,
):
    """
    Build and upload GitHub repository algorithm image

    Args:
        algorithm: Algorithm name
        image: Docker image name
        tag: Image tag
        repo: GitHub repository (owner/repo)
        branch: Git branch name
        path: Sub-directory path in repository
        force_rebuild: Whether to force rebuild
    """

    load_env_file()
    config = get_config()

    token = os.getenv(TOKEN_KEY)
    if token is None:
        logger.error(f"Environment variable {TOKEN_KEY} not set. Please set it to your GitHub token.")
        return

    resp = None
    try:
        configuration = get_configuration(host=config.base_url)
        with ApiClient(configuration=configuration) as client:
            api = AlgorithmApi(api_client=client)
            resp = api.api_v1_algorithms_build_post(
                algorithm=algorithm,
                image=image,
                tag=tag,
                source_type="github",
                github_token=token,
                github_repo=repo,
                github_branch=branch,
                github_path=path,
                force_rebuild=force_rebuild,
            )
    except (BadRequestException, NotFoundException, ServiceException) as e:
        logger.error(f"Failed to build algorithm '{algorithm}': {e}")
        return

    if resp is not None:
        get_algorithm_item(host=config.base_url, response=resp)


if __name__ == "__main__":
    app()
