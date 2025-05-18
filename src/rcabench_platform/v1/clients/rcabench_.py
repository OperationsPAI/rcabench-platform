from ..logging import logger

from typing import Any, Literal

import requests

BASE_URL = "http://10.10.10.220:32080"


class CustomRCABenchSDK:
    def __init__(self, base_url: str = BASE_URL) -> None:
        self.api_url = base_url.rstrip("/") + "/api/v1"
        self.client = requests.Session()

    def query_dataset(self, name: str, sort: Literal["desc", "asc"] = "desc") -> dict[str, Any]:
        path = "/datasets/query"
        query = {"name": name, "sort": sort}

        resp = self.client.get(self.api_url + path, params=query)
        resp.raise_for_status()

        resp_json = resp.json()
        return resp_json["data"]
