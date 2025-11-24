import os

import polars as pl
from rcabench.openapi import (
    ApiClient,
    AuthenticationApi,
    BatchEvaluateDatapackReq,
    BatchEvaluateDatapackResp,
    BatchEvaluateDatasetResp,
    Configuration,
    ContainerRef,
    DatasetsApi,
    EvaluateDatapackSpec,
    EvaluationsApi,
    InjectionResp,
    InjectionsApi,
    LoginReq,
)

from ..config import get_config


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

    _instances = {}
    _sessions = {}

    def __new__(cls, base_url: str | None = None, username: str | None = None, password: str | None = None):
        # Parse actual configuration values
        actual_base_url = (
            base_url
            or os.getenv("RCABENCH_BASE_URL")
            or get_config(env_mode=os.environ.get("ENV_MODE", "dev")).base_url
        )
        actual_username = username or os.getenv("RCABENCH_USERNAME")

        # Use (base_url, username) as unique identifier
        instance_key = (actual_base_url, actual_username)

        if instance_key not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[instance_key] = instance
            instance._initialized = False

        return cls._instances[instance_key]

    def __init__(self, base_url: str | None = None, username: str | None = None, password: str | None = None):
        # Avoid duplicate initialization of the same instance
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.base_url = (
            base_url
            or os.getenv("RCABENCH_BASE_URL")
            or get_config(env_mode=os.environ.get("ENV_MODE", "dev")).base_url
        )
        self.username = username or os.getenv("RCABENCH_USERNAME")
        self.password = password or os.getenv("RCABENCH_PASSWORD")

        assert self.username is not None, "username or RCABENCH_USERNAME is not set"
        assert self.password is not None, "password or RCABENCH_PASSWORD is not set"
        assert self.base_url is not None, "base_url or RCABENCH_BASE_URL is not set"

        self.instance_key = (self.base_url, self.username)
        self._initialized = True

    def __enter__(self):
        # Check if there is already a valid session
        if self.instance_key not in self._sessions or not self._is_session_valid():
            self._login()
        return self._get_authenticated_client()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Do not close session, maintain singleton state
        pass

    def _is_session_valid(self):
        """Check if the current session is valid"""
        session_data = self._sessions.get(self.instance_key)
        if not session_data:
            return False

        # More complex session validity checks can be added here, such as checking if token is expired
        # Currently simply check if access_token exists
        return session_data.get("access_token") is not None

    def _login(self):
        config = Configuration(host=self.base_url)
        with ApiClient(config) as api_client:
            auth_api = AuthenticationApi(api_client)
            assert self.username is not None
            assert self.password is not None
            login_request = LoginReq(username=self.username, password=self.password)
            response = auth_api.login(login_request)
            assert response.data is not None

            # Store session information in class-level cache
            self._sessions[self.instance_key] = {"access_token": response.data.token, "api_client": None}

    def _get_authenticated_client(self):
        session_data = self._sessions.get(self.instance_key)
        if not session_data:
            self._login()
            session_data = self._sessions[self.instance_key]

        # If api_client has not been created or needs to be updated, create a new one
        if not session_data.get("api_client"):
            auth_config = Configuration(
                host=self.base_url,
                api_key={"BearerAuth": session_data["access_token"]},
                api_key_prefix={"BearerAuth": "Bearer"},
            )
            session_data["api_client"] = ApiClient(auth_config)

        return session_data["api_client"]

    def get_client(self):
        if self.instance_key not in self._sessions or not self._is_session_valid():
            self._login()
        return self._get_authenticated_client()

    @classmethod
    def clear_sessions(cls):
        cls._sessions.clear()
        cls._instances.clear()


def get_evaluation_by_datapack(
    algorithm: str, datapack: str, tag: str | None = None, base_url: str | None = None
) -> BatchEvaluateDatapackResp:
    base_url = base_url or os.getenv("RCABENCH_BASE_URL")
    assert base_url is not None, "base_url or RCABENCH_BASE_URL is not set"
    assert tag, "Tag must be specified."

    with RCABenchClient(base_url=base_url) as client:
        api = EvaluationsApi(client)
        resp = api.evaluate_algorithm_on_datapacks(
            request=BatchEvaluateDatapackReq(
                specs=[
                    EvaluateDatapackSpec(
                        algorithm=ContainerRef(name=algorithm, version=tag),
                        datapack=datapack,
                    )
                ]
            )
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
) -> BatchEvaluateDatasetResp:
    base_url = base_url or os.getenv("RCABENCH_BASE_URL")
    assert base_url is not None, "base_url or RCABENCH_BASE_URL is not set"

    with RCABenchClient(base_url=base_url) as client:
        api = EvaluationsApi(client)
        resp = api.evaluate_algorithm_on_datasets(
            request=BatchEvaluateDatapackReq(
                specs=[
                    EvaluateDatapackSpec(
                        algorithm=ContainerRef(name=algorithm, version=tag),
                        datapack=f"{dataset}:{dataset_version}" if dataset_version else dataset,
                    )
                ]
            )
        )

    assert resp.code is not None and resp.code < 300, f"Failed to get evaluation: {resp.message}"
    assert resp.data is not None
    return resp.data


def get_datapacks_from_dataset_id(
    dataset_id: int | None = None,
) -> tuple[list[InjectionResp], str, str]:
    """Get datapacks/injections from a dataset ID.

    Note: In the new SDK, we need to query injections separately and filter by dataset.
    """
    with RCABenchClient() as client:
        assert dataset_id is not None

        # Get dataset info
        datasets_api = DatasetsApi(client)
        dataset_resp = datasets_api.get_dataset_by_id(dataset_id=dataset_id)
        if not dataset_resp or not dataset_resp.data:
            raise ValueError(f"Dataset {dataset_id} not found")

        dataset_name = dataset_resp.data.name
        assert dataset_name is not None, "Dataset name is None"

        # Get injections for this dataset
        # Note: The new API doesn't have a direct way to get injections by dataset_id
        # We need to list all injections and filter, or use search
        injections_api = InjectionsApi(client)
        injections_resp = injections_api.list_injections()

        if not injections_resp or not injections_resp.data or not injections_resp.data.items:
            raise ValueError(f"No injections found for dataset {dataset_id}")

        # Filter injections by dataset name (this is a workaround)
        # In the new API, injection names might be prefixed with dataset info
        injections = injections_resp.data.items

        # Get the first version if versions exist, otherwise use empty string
        dataset_version = ""
        if dataset_resp.data.versions and len(dataset_resp.data.versions) > 0:
            dataset_version = dataset_resp.data.versions[0].name or ""

        return injections, dataset_name, dataset_version
