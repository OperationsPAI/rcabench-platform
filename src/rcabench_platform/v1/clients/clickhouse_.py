import clickhouse_connect.driver.client
import clickhouse_connect


type ClickHouseClient = clickhouse_connect.driver.client.Client


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
