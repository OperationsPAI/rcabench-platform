from ..spec.data import TEMP
from ..utils.serde import save_csv
from ..spec.algorithm import AlgorithmArgs
from ..cli.main import app
from ..cli.eval import ALGORITHMS
from ..logging import timeit, logger

from ..datasets.convert_rcabench import RcabenchDatapackLoader
from ..spec.convert import convert_datapack

from pathlib import Path
import json
import os

import polars as pl


@app.command()
@timeit()
def run():
    algorithm = str(os.environ["ALGORITHM"])
    input_path = Path(os.environ["INPUT_PATH"])
    output_path = Path(os.environ["OUTPUT_PATH"])

    assert algorithm in ALGORITHMS, f"Unknown algorithm: {algorithm}"
    assert input_path.is_dir()
    assert output_path.is_dir()

    with open(input_path / "injection.json") as f:
        injection = json.load(f)
        injection_name = injection["injection_name"]
        assert isinstance(injection_name, str) and injection_name

    converted_input_path = output_path / "converted"

    convert_datapack(
        loader=RcabenchDatapackLoader(src_folder=input_path, datapack=injection_name),
        dst_folder=converted_input_path,
        skip=True,
    )

    a = ALGORITHMS[algorithm]()

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
    assert algorithm in ALGORITHMS

    input_path = Path("data") / "rcabench_dataset" / datapack

    output_path = TEMP / "run_exp_platform" / datapack / algorithm
    output_path.mkdir(parents=True, exist_ok=True)

    os.environ["ALGORITHM"] = algorithm
    os.environ["INPUT_PATH"] = str(input_path)
    os.environ["OUTPUT_PATH"] = str(output_path)

    run()


if __name__ == "__main__":
    app()
