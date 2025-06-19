from ..config import get_config
from ..logging import logger

from typing import Any, Literal
from pprint import pprint

import requests
import rcabench.rcabench
import mysql.connector.abstracts
import mysql.connector


def get_rcabench_sdk() -> rcabench.rcabench.RCABenchSDK:
    return rcabench.rcabench.RCABenchSDK(base_url=get_config().base_url)


def get_mariadb_connection() -> mysql.connector.abstracts.MySQLConnectionAbstract:
    conn = mysql.connector.connect(**get_config().database.__dict__)

    assert isinstance(conn, mysql.connector.abstracts.MySQLConnectionAbstract)
    assert conn.is_connected()
    return conn


class CustomRCABenchSDK:
    def __init__(self, base_url: str | None = None) -> None:
        if base_url is None:
            base_url = get_config().base_url

        self.api_url = base_url.rstrip("/") + "/api/v1"
        self.client = requests.Session()

    def query_dataset(self, name: str, sort: Literal["desc", "asc"] = "desc") -> dict[str, Any]:
        path = "/datasets/query"
        query = {"name": name, "sort": sort}

        resp = self.client.get(self.api_url + path, params=query)
        resp.raise_for_status()

        resp_json = resp.json()
        return resp_json["data"]

    def query_injection(self, name: str) -> dict[str, Any]:
        with get_mariadb_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT * FROM fault_injection_schedules WHERE injection_name = %s",
                (name,),
            )

            rows = cursor.fetchall()
            assert isinstance(rows, list)
            assert len(rows) == 1
            assert isinstance(rows[0], dict)
            return rows[0]
