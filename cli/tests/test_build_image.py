#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.logging import timeit
from rcabench_platform.v2.config import get_config

from collections.abc import Generator
from pathlib import Path
import dotenv
import os

from rcabench.openapi.api import AlgorithmApi
from rcabench.const import EventType
from rcabench.model.error import ModelHTTPError
from rcabench.const import TaskStatus
from rcabench.model.trace import AlgorithmItem, InfoPayload, StreamEvent
from rcabench.openapi.api_client import ApiClient, Configuration
from rcabench.openapi.models.dto_generic_response_dto_submit_resp import DtoGenericResponseDtoSubmitResp
from rcabench.rcabench import RCABenchSDK

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
                item = AlgorithmItem.model_validate_json(event.payload)
                logger.info(f"Build succeeded: {item}")
            elif event.event_name == EventType.EventTaskStatusUpdate:
                assert event.payload is not None
                item = InfoPayload.model_validate_json(event.payload)
                if item.status == TaskStatus.ERROR:
                    logger.error(f"Build failed: {item.message}")


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
    host: str, trace_id: str, timeout: float | None = 600
) -> Generator[StreamEvent | ModelHTTPError, None, None]:
    sdk = RCABenchSDK(host)
    return sdk.trace.stream_trace_events(trace_id, timeout=timeout)


# @app.command()
# @timeit()
# def local(
#     env: str = "prod",
#     algorithm: str = "traceback",
#     filename: str = "traceback.zip",
#     image: str = "10.10.10.240/library/rca-algo-traceback-local-file",
#     tag: str = "latest",
#     force_rebuild: bool = False,
# ):
#     """
#     Build and upload local file algorithm image

#     Args:
#         env: Environment mode (prod or dev or debug)
#         filename: Source code filename
#         algorithm: Algorithm name
#         image: Docker image name
#         tag: Image tag
#         force_rebuild: Whether to force rebuild
#     """

#     config = get_config(env_mode=env)

#     file_dir = config.temp
#     file_path = file_dir / filename
#     if not file_path.exists():
#         logger.error(f"File {file_path} does not exist. Please provide a valid file.")
#         raise FileNotFoundError(f"{file_path} does not exist.")

#     with open(file_path, "rb") as f:
#         file_content = f.read()

#     resp = None
#     configuration = get_configuration(host=config.base_url)
#     with ApiClient(configuration=configuration) as client:
#         api = AlgorithmApi(api_client=client)
#         resp = api.api_v1_algorithms_build_post( # FIXME
#             algorithm=algorithm,
#             image=image,
#             tag=tag,
#             source_type="file",
#             file=(filename, file_content),
#             force_rebuild=force_rebuild,
#         )

#     assert resp is not None
#     get_algorithm_item(host=config.base_url, response=resp)


# @app.command()
# @timeit()
# def github(
#     env: str = "prod",
#     algorithm: str = "traceback",
#     image: str = "10.10.10.240/library/rca-algo-traceback-github",
#     tag: str = "latest",
#     repo: str = "LGU-SE-Internal/rca-algo-contrib",
#     branch: str = "main",
#     path: str = "algorithms/traceback",
#     force_rebuild: bool = False,
# ):
#     """
#     Build and upload GitHub repository algorithm image

#     Args:
#         env: Environment mode (prod or dev or debug)
#         algorithm: Algorithm name
#         image: Docker image name
#         tag: Image tag
#         repo: GitHub repository (owner/repo)
#         branch: Git branch name
#         path: Sub-directory path in repository
#         force_rebuild: Whether to force rebuild
#     """

#     set_token()
#     config = get_config(env_mode=env)

#     token = os.getenv(TOKEN_KEY)
#     if token is None:
#         logger.error(f"Environment variable {TOKEN_KEY} not set. Please set it to your GitHub token.")
#         raise ValueError(f"Environment variable {TOKEN_KEY} not set.")

#     resp = None
#     configuration = get_configuration(host=config.base_url)
#     with ApiClient(configuration=configuration) as client:
#         api = AlgorithmApi(api_client=client)
#         resp = api.api_v1_algorithms_build_post( # FIXME
#             algorithm=algorithm,
#             image=image,
#             tag=tag,
#             source_type="github",
#             github_token=token,
#             github_repo=repo,
#             github_branch=branch,
#             github_path=path,
#             force_rebuild=force_rebuild,
#         )

#     assert resp is not None
#     get_algorithm_item(host=config.base_url, response=resp)


if __name__ == "__main__":
    app()
