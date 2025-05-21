from ..spec.data import DATA_ROOT
from ..clients.rcabench_ import get_rcabench_sdk
from ..clients.clickhouse_ import get_clickhouse_client
from ..logging import logger, timeit

import traceback

import typer

app = typer.Typer()


@app.command()
@timeit()
def ping_clickhouse() -> None:
    with get_clickhouse_client() as client:
        assert client.ping(), "clickhouse should be reachable"
        logger.info("clickhouse is reachable")


@app.command()
@timeit()
def ping_rcabench() -> None:
    sdk = get_rcabench_sdk()
    sdk.injection.list()
    logger.info("rcabench is reachable")


@app.command()
@timeit()
def test() -> None:
    logger.info("Testing rcabench-platform environment...")

    try:
        ping_clickhouse()
    except Exception as e:
        traceback.print_exc()
        logger.error(f"ClickHouse ping failed: {e}")

    try:
        ping_rcabench()
    except Exception as e:
        traceback.print_exc()
        logger.error(f"RCABench ping failed: {e}")

    try:
        DATA_ROOT.stat()
        logger.info(f"DATA_ROOT is found: {DATA_ROOT}")
    except Exception as e:
        traceback.print_exc()
        logger.error(f"DATA_ROOT is not found: {e}")

    logger.info("Hello from rcabench-platform!")
