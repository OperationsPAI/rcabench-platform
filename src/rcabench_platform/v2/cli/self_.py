from ..config import get_config
from ..clients.rcabench_ import RcabenchSdkHelper, get_rcabench_sdk
from ..clients.clickhouse import get_clickhouse_client
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
    sdk = RcabenchSdkHelper()
    sdk.get_analysis_with_issues()
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
        config = get_config()
        config.data.stat()
        logger.opt(colors=True).info(f"config.data is found: <green>{config.data}</green>")
    except Exception as e:
        traceback.print_exc()
        logger.error(f"config.data is not found: {e}")

    logger.info("Hello from rcabench-platform!")
