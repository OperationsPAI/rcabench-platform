import rcabench.rcabench
from rcabench.openapi import ApiClient, Configuration

from ..config import get_config


def get_rcabench_sdk(*, base_url: str | None = None) -> rcabench.rcabench.RCABenchSDK:
    if base_url is None:
        base_url = get_config().base_url

    return rcabench.rcabench.RCABenchSDK(base_url=base_url)


def get_rcabench_openapi_client(*, base_url: str | None = None) -> ApiClient:
    if base_url is None:
        base_url = get_config().base_url

    return ApiClient(configuration=Configuration(host=base_url))
