import os

from rcabench.openapi import (
    ApiClient,
    AuthenticationApi,
    Configuration,
    DtoAlgorithmDatapackEvaluationResp,
    DtoAlgorithmDatasetEvaluationResp,
    EvaluationApi,
)
from rcabench.openapi.models.dto_login_request import DtoLoginRequest
from rcabench.rcabench import RCABenchSDK

from ..config import get_config


def get_rcabench_sdk(*, base_url: str | None = None) -> RCABenchSDK:
    if base_url is None:
        base_url = get_config().base_url

    return RCABenchSDK(base_url=base_url)


def get_rcabench_openapi_client(*, base_url: str | None = None) -> ApiClient:
    if base_url is None:
        base_url = get_config().base_url

    return ApiClient(configuration=Configuration(host=base_url))


class RCABenchClient:
    """
    Usage:
    with RCABenchClient() as api_client:
        container_api = rcabench.openapi.ContainersApi(api_client)
        containers = container_api.api_v2_containers_get()
        print(f"Containers: {containers.data}")
    """

    def __init__(self, base_url: str | None = None, username: str | None = None, password: str | None = None):
        self.base_url = (
            base_url or os.getenv("RCABENCH_BASE_URL") or get_config(env_mode=os.environ["ENV_MODE"]).base_url
        )
        self.username = username or os.getenv("RCABENCH_USERNAME")
        self.password = password or os.getenv("RCABENCH_PASSWORD")

        assert self.username is not None, "username or RCABENCH_USERNAME is not set"
        assert self.password is not None, "password or RCABENCH_PASSWORD is not set"
        assert self.base_url is not None, "base_url or RCABENCH_BASE_URL is not set"

        self.access_token = None
        self._api_client = None

    def __enter__(self):
        self._login()
        return self._get_authenticated_client()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _login(self):
        if self.access_token:
            return

        config = Configuration(host=self.base_url)
        with ApiClient(config) as api_client:
            auth_api = AuthenticationApi(api_client)
            assert self.username is not None
            assert self.password is not None
            login_request = DtoLoginRequest(username=self.username, password=self.password)
            response = auth_api.api_v2_auth_login_post(login_request)
            assert response.data is not None
            self.access_token = response.data.token

    def _get_authenticated_client(self):
        if not self.access_token:
            self._login()

        auth_config = Configuration(
            host=self.base_url,
            api_key={"BearerAuth": self.access_token} if self.access_token else None,
            api_key_prefix={"BearerAuth": "Bearer"},
        )

        self._api_client = ApiClient(auth_config)
        return self._api_client

    def get_client(self):
        if not self._api_client:
            self._api_client = self._get_authenticated_client()
        return self._api_client


def get_evaluation_by_datapack(
    algorithm: str, datapack: str, tag: str | None = None, base_url: str | None = None
) -> DtoAlgorithmDatapackEvaluationResp:
    base_url = base_url or os.getenv("RCABENCH_BASE_URL")
    assert base_url is not None, "base_url or RCABENCH_BASE_URL is not set"
    assert tag, "Tag must be specified."

    with RCABenchClient(base_url=base_url) as client:
        api = EvaluationApi(client)
        resp = api.api_v2_evaluations_algorithms_algorithm_datapacks_datapack_get(
            algorithm=algorithm,
            datapack=datapack,
            tag=tag,
        )

    assert resp.code is not None and resp.code < 300, f"Failed to get evaluation: {resp.message}"
    assert resp.data is not None
    return resp.data


def get_evaluation_by_dataset(
    algorithm: str,
    dataset: str,
    dataset_version: str | None = None,
    tag: str | None = None,
    base_url: str | None = None,
) -> DtoAlgorithmDatasetEvaluationResp:
    base_url = base_url or os.getenv("RCABENCH_BASE_URL")
    assert base_url is not None, "base_url or RCABENCH_BASE_URL is not set"
    assert tag, "Tag must be specified."

    with RCABenchClient(base_url=base_url) as client:
        api = EvaluationApi(client)
        resp = api.api_v2_evaluations_algorithms_algorithm_datasets_dataset_get(
            algorithm=algorithm,
            dataset=dataset,
            dataset_version=dataset_version,
            tag=tag,
        )

    assert resp.code is not None and resp.code < 300, f"Failed to get evaluation: {resp.message}"
    assert resp.data is not None
    return resp.data
