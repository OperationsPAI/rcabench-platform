from typing import TypeAlias
from pathlib import Path

import clickhouse_connect.driver.client
import clickhouse_connect

ClickHouseClient: TypeAlias = clickhouse_connect.driver.client.Client


def get_clickhouse_client() -> ClickHouseClient:
    host = "10.10.10.58"
    username = "default"
    password = "password"
    database = "default"

    client = clickhouse_connect.get_client(
        host=host,
        username=username,
        password=password,
        database=database,
    )

    return client


def query_parquet_stream(client: ClickHouseClient, query: str, save_path: Path):
    assert save_path.suffix == ".parquet", "save_path must be a parquet file"
    assert save_path.parent.is_dir(), "save_path parent must be a directory"

    stream = client.raw_stream(query=query, fmt="Parquet")
    with open(save_path, "wb") as f:
        for chunk in stream:
            f.write(chunk)
        f.flush()
