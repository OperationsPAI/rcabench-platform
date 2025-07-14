from ..logging import timeit
from ..config import get_config

from ..algorithms.spec import global_algorithm_registry
from ..algorithms.spec import AlgorithmArgs

from ..sources.rcabench import RcabenchDatapackLoader
from ..sources.convert import convert_datapack

from ..utils.serde import save_csv, load_json

from typing import Annotated
from pathlib import Path

import polars as pl
import typer

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

    answers = a(
        AlgorithmArgs(
            dataset="rcabench",
            datapack=injection_name,
            input_folder=converted_input_path,
            output_folder=output_path,
        )
    )

    result_rows = [{"level": ans.level, "result": ans.name, "rank": ans.rank, "confidence": 0} for ans in answers]
    result_df = pl.DataFrame(result_rows).sort(by=["rank"])
    save_csv(result_df, path=output_path / "result.csv")


@app.command()
@timeit()
def local_test(algorithm: str, datapack: str):
    input_path = Path("data") / "rcabench_dataset" / datapack

    output_path = get_config().temp / "run_exp_platform" / datapack / algorithm
    output_path.mkdir(parents=True, exist_ok=True)

    run(algorithm, input_path, output_path)
