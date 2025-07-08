#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app
from rcabench_platform.v2.logging import timeit
from rcabench.openapi.api import AlgorithmApi
from rcabench.const import EventType
from rcabench.model.error import ModelHTTPError
from rcabench.model.trace import AlgorithmItem, StreamEvent
from rcabench.openapi.api_client import ApiClient, Configuration
from rcabench.openapi.models.dto_generic_response_dto_submit_resp import DtoGenericResponseDtoSubmitResp
from rcabench.rcabench import RCABenchSDK

from collections.abc import Generator
from pathlib import Path

import json


def get_configuration():
    configuration = Configuration(host="http://10.10.10.220:32080")
    configuration.datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    return configuration


def load_env_file() -> dict[str, str]:
    env_file = Path(".env")

    env_vars = {}
    try:
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip().strip('"').strip("'")
    except FileNotFoundError:
        print(f"Warning: {env_file} not found")

    return env_vars


def trace_execution(
    host: str, trace_id: str, timeout: float | None = 300
) -> Generator[StreamEvent | ModelHTTPError, None, None]:
    sdk = RCABenchSDK(host)
    return sdk.trace.stream_trace_events(trace_id, timeout=timeout)


def get_algorithm_item(response: DtoGenericResponseDtoSubmitResp):
    assert response.data is not None
    print("Build image response:", response)

    traces = response.data.traces
    assert traces is not None

    trace_ids = [trace.trace_id for trace in traces]
    assert trace_ids[0] is not None
    res = trace_execution(host="http://10.10.10.220:32080", trace_id=trace_ids[0])
    for event in res:
        if isinstance(event, ModelHTTPError):
            print("Error:", event.detail)
        else:
            if event.event_name == EventType.EventImageBuildSucceed:
                assert event.payload is not None
                payload = json.loads(event.payload)
                item = AlgorithmItem.model_validate(payload)
                print("Build succeeded:", item)


@app.command()
@timeit()
def local():
    filename = "traceback.zip"
    file_path = Path("data") / filename
    with open(file_path, "rb") as f:
        file_content = f.read()

    with ApiClient(get_configuration()) as client:
        api = AlgorithmApi(api_client=client)
        response = api.api_v1_algorithms_build_post(
            algorithm="traceback",
            image="10.10.10.240/library/rca-algo-traceback-local-file",
            tag="latest",
            source_type="file",
            file=(filename, file_content),
            force_rebuild=False,
        )

    get_algorithm_item(response)


@app.command()
@timeit()
def github():
    env_vars = load_env_file()
    token = env_vars.get("GITHUB_TOKEN", "")

    with ApiClient(get_configuration()) as client:
        api = AlgorithmApi(api_client=client)
        response = api.api_v1_algorithms_build_post(
            algorithm="traceback",
            image="10.10.10.240/library/rca-algo-traceback-github",
            tag="latest",
            source_type="github",
            github_token=token,
            github_repo="LGU-SE-Internal/rca-algo-contrib",
            github_branch="main",
            github_path="algorithms/traceback",
            force_rebuild=False,
        )

    get_algorithm_item(response)


if __name__ == "__main__":
    app()
