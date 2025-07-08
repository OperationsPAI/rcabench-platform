from ..config import get_config
from ..cli.main import app, logger, timeit
from ..cli.main import register_builtin_algorithms
from ..algorithms.spec import global_algorithm_registry
from ..sources.rcabench import RcabenchDatapackLoader
from ..sources.convert import convert_datapack
from ..algorithms.spec import AlgorithmArgs
from ..utils.serde import save_csv

from pathlib import Path
import json
import os

import polars as pl


@app.command()
@timeit()
def run(*, enable_builtin_algorithms: bool = True):
    if enable_builtin_algorithms:
        register_builtin_algorithms()

    algorithm = str(os.environ["ALGORITHM"])
    input_path = Path(os.environ["INPUT_PATH"])
    output_path = Path(os.environ["OUTPUT_PATH"])

    assert algorithm in global_algorithm_registry(), f"Unknown algorithm: {algorithm}"
    assert input_path.is_dir(), f"input_path: {input_path}"
    assert output_path.is_dir(), f"output_path: {output_path}"

    with open(input_path / "injection.json") as f:
        injection = json.load(f)
        injection_name = injection["injection_name"]
        assert isinstance(injection_name, str) and injection_name

    converted_input_path = output_path / "converted"

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

    os.environ["ALGORITHM"] = algorithm
    os.environ["INPUT_PATH"] = str(input_path)
    os.environ["OUTPUT_PATH"] = str(output_path)

    run()


if __name__ == "__main__":
    app()
