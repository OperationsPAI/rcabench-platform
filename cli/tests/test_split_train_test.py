#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.datasets.rcabench import rcabench_split_train_test
from rcabench_platform.v2.datasets.spec import delete_dataset, get_datapack_list


@app.command()
@timeit()
def run():
    datapacks = get_datapack_list("rcabench_with_issues")

    rcabench_split_train_test(
        datapacks=datapacks[: len(datapacks) // 2],
        train_ratio=0.8,
        train_dataset_name="__dev__rcabench_train_r1",
        test_dataset_name="__dev__rcabench_test_r1",
        previous_train_datapacks=[],
        previous_test_datapacks=[],
    )

    rcabench_split_train_test(
        datapacks=datapacks,
        train_ratio=0.8,
        train_dataset_name="__dev__rcabench_train_r2",
        test_dataset_name="__dev__rcabench_test_r2",
        previous_train_datapacks=get_datapack_list("__dev__rcabench_train_r1"),
        previous_test_datapacks=get_datapack_list("__dev__rcabench_test_r1"),
    )

    logger.info("Test completed successfully.")


@app.command()
@timeit()
def cleanup():
    delete_dataset("__dev__rcabench_train_r1")
    delete_dataset("__dev__rcabench_test_r1")
    delete_dataset("__dev__rcabench_train_r2")
    delete_dataset("__dev__rcabench_test_r2")


if __name__ == "__main__":
    app()
