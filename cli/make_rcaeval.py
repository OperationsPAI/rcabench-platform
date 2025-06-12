#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.sources.convert import convert_datapack, convert_dataset
from rcabench_platform.v2.sources.rcaeval import RcaevalDatapackLoader, RcaevalDatasetLoader

from pathlib import Path


@app.command()
@timeit()
def run(skip_finished: bool = True, parallel: int = 4):
    src_root = Path("data") / "RCAEval"
    src_datasets = ["RE2-TT", "RE2-OB"]

    for src_dataset in src_datasets:
        dst_dataset = "rcaeval_" + src_dataset.lower().replace("-", "_")
        loader = RcaevalDatasetLoader(src_folder=src_root / src_dataset, dataset=dst_dataset)
        convert_dataset(loader, skip_finished=skip_finished, parallel=parallel)


@app.command()
@timeit()
def local_test_1():
    loader = RcaevalDatapackLoader(
        src_folder=Path("data/RCAEval/RE2-TT/ts-auth-service_cpu/1"),
        dataset="rcaeval_re2_tt",
        datapack="ts-auth-service_cpu_1",
        service="ts-auth-service",
    )
    convert_datapack(
        loader,
        dst_folder=Path("temp/rcaeval_re2_tt/ts-auth-service_cpu_1"),
        skip_finished=False,
    )


@app.command()
@timeit()
def local_test_2():
    loader = RcaevalDatapackLoader(
        src_folder=Path("data/RCAEval/RE2-OB/checkoutservice_cpu/1"),
        dataset="rcaeval_re2_ob",
        datapack="checkoutservice_cpu_1",
        service="checkoutservice",
    )
    convert_datapack(
        loader,
        dst_folder=Path("temp/rcaeval_re2_ob/checkoutservice_cpu_1"),
        skip_finished=False,
    )


if __name__ == "__main__":
    app()
