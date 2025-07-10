from ..config import get_config

import rcabench.rcabench

from rcabench.openapi import (
    ApiClient,
    Configuration,
    InjectionApi,
    DtoQueryInjectionResp,
    DtoFaultInjectionWithIssuesResp,
    DatabaseFaultInjectionSchedule,
)


def get_rcabench_sdk(base_url: str | None = None) -> rcabench.rcabench.RCABenchSDK:
    if base_url is None:
        base_url = get_config().base_url

    return rcabench.rcabench.RCABenchSDK(base_url=base_url)


def get_rcabench_openapi_client(base_url: str | None = None) -> ApiClient:
    if base_url is None:
        base_url = get_config().base_url

    return ApiClient(configuration=Configuration(host=base_url))


class RcabenchSdkHelper:
    def __init__(self, api_client: ApiClient | None = None) -> None:
        self.api_client = api_client or get_rcabench_openapi_client()

    @classmethod
    def from_base_url(cls, base_url: str | None = None) -> "RcabenchSdkHelper":
        api_client = get_rcabench_openapi_client(base_url=base_url)
        return cls(api_client=api_client)

    def get_injection_details(self, *, dataset_name: str) -> DtoQueryInjectionResp:
        api = InjectionApi(self.api_client)
        resp = api.api_v1_injections_query_get(name=dataset_name)
        assert resp.data is not None
        return resp.data

    def get_analysis_with_issues(self) -> list[DtoFaultInjectionWithIssuesResp]:
        api = InjectionApi(self.api_client)
        resp = api.api_v1_injections_analysis_with_issues_get()
        assert resp.data is not None
        return resp.data

    def list_injections(self) -> list[DatabaseFaultInjectionSchedule]:
        api = InjectionApi(self.api_client)
        resp = api.api_v1_injections_get()
        assert resp.data is not None
        return resp.data
