import os
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
from rcabench.openapi import AlgorithmsApi, DtoGranularityResultEnhancedRequest, DtoGranularityResultItem

from ..algorithms.spec import AlgorithmArgs, global_algorithm_registry
from ..clients.rcabench_ import RCABenchClient
from ..config import get_config
from ..logging import logger, timeit
from ..sources.convert import convert_datapack
from ..sources.rcabench import RcabenchDatapackLoader
from ..utils.serde import load_json, save_csv

app = typer.Typer()


@app.command()
@timeit()
def run(
    algorithm: Annotated[str, typer.Option("-a", "--algorithm", envvar="ALGORITHM")],
    input_path: Annotated[Path, typer.Option("-i", "--input-path", envvar="INPUT_PATH")],
    output_path: Annotated[Path, typer.Option("-o", "--output-path", envvar="OUTPUT_PATH")],
):
    assert algorithm in global_algorithm_registry(), f"Unknown algorithm: {algorithm}"
    assert input_path.is_dir(), f"input_path: {input_path}"
    assert output_path.is_dir(), f"output_path: {output_path}"

    injection = load_json(path=input_path / "injection.json")
    injection_name = injection["injection_name"]
    assert isinstance(injection_name, str) and injection_name

    converted_input_path = input_path / "converted"

    convert_datapack(
        loader=RcabenchDatapackLoader(src_folder=input_path, datapack=injection_name),
        dst_folder=converted_input_path,
        skip_finished=True,
    )

    a = global_algorithm_registry()[algorithm]()

    start_time = datetime.now()
    answers = a(
        AlgorithmArgs(
            dataset="rcabench",
            datapack=injection_name,
            input_folder=converted_input_path,
            output_folder=output_path,
        )
    )
    duration = datetime.now() - start_time

    result_rows = [{"level": ans.level, "result": ans.name, "rank": ans.rank, "confidence": 0} for ans in answers]

    algorithm_id_str = os.environ.get("ALGORITHM_ID")
    execution_id_str = os.environ.get("EXECUTION_ID")
    assert algorithm_id_str is not None, "ALGORITHM_ID is not set"
    algorithm_id = int(algorithm_id_str)

    if execution_id_str is not None:
        execution_id = int(execution_id_str)
    else:
        execution_id = None

    with RCABenchClient() as client:
        algo_api = AlgorithmsApi(client)

        resp = algo_api.api_v2_algorithms_algorithm_id_results_post(
            algorithm_id=algorithm_id,
            execution_id=execution_id,
            request=DtoGranularityResultEnhancedRequest(
                duration=duration.total_seconds(),
                results=[
                    DtoGranularityResultItem(
                        level=row["level"],
                        result=row["result"],
                        rank=row["rank"],
                        confidence=row["confidence"],
                    )
                    for row in result_rows
                ],
            ),
        )
        logger.info(f"Submit detector result: response code: {resp.code}, message: {resp.message}")


@app.command()
@timeit()
def local_test(algorithm: str, datapack: str):
    input_path = Path("data") / "rcabench_dataset" / datapack

    output_path = get_config().temp / "run_exp_platform" / datapack / algorithm
    output_path.mkdir(parents=True, exist_ok=True)

    run(algorithm, input_path, output_path)
