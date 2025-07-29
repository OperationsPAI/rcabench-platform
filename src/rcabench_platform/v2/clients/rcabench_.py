from rcabench.openapi import ApiClient, AuthenticationApi, Configuration
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

    def __init__(self, base_url="http://10.10.10.220:32080", username="admin", password="admin123"):
        self.base_url = base_url
        self.username = username
        self.password = password
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
