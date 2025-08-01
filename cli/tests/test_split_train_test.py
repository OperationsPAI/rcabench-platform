#!/usr/bin/env -S uv run -s
from rcabench.openapi import DatasetsApi, DtoDatasetV2CreateReq, DtoInjectionRef

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.config import get_config
from rcabench_platform.v2.datasets.rcabench import rcabench_split_train_test
from rcabench_platform.v2.datasets.spec import delete_dataset, get_datapack_list


def get_previous_datapacks(datasets_api: DatasetsApi) -> list[str]:
    resp = datasets_api.api_v2_datasets_get(search="pair-diag")
    assert resp.code is not None and resp.code < 300 and resp.data is not None and resp.data.items is not None
    dataset = resp.data.items[0]

    if dataset.injections is not None:
        previous_datapacks = [
            item.fault_injection.injection_name
            for item in dataset.injections
            if item.fault_injection is not None and item.fault_injection.injection_name is not None
        ]
        assert len(previous_datapacks) > 0
        return previous_datapacks
    return []


def split_datapacks(datapacks: list, previous_datapacks: list[str]) -> tuple[list, list]:
    train_datapacks, test_datapacks = rcabench_split_train_test(
        datapacks=datapacks[:300],
        train_ratio=0.8,
        previous_datapacks=previous_datapacks,
    )
    return train_datapacks, test_datapacks


def create_dataset(
    datasets_api: DatasetsApi, name: str, dataset_type: str, version: str, description: str, datapacks: list
) -> bool:
    resp = datasets_api.api_v2_datasets_post(
        dataset=DtoDatasetV2CreateReq(
            name=name,
            type=dataset_type,
            version=version,
            description=description,
            data_source="train-ticket",
            injection_refs=[DtoInjectionRef(name=name) for name in datapacks],
        ),
    )

    logger.info(f"request dataset creation: code[{resp.code}], message[{resp.message}]")
    if resp.code is not None and resp.code < 205:
        assert resp.data is not None
        logger.info(f"dataset created: id[{resp.data.id}], name[{resp.data.name}]")
        return True
    return False


@app.command()
@timeit()
def run(stage: int):
    datapacks = get_datapack_list("rcabench")  # can be replaced by query with issues

    with RCABenchClient(base_url=get_config().base_url) as client:
        datasets_api = DatasetsApi(client)

        previous_datapacks = get_previous_datapacks(datasets_api)

        train_datapacks, test_datapacks = rcabench_split_train_test(
            datapacks=datapacks,
            train_ratio=0.8,
            previous_datapacks=previous_datapacks,
            datapack_limit=300,
        )

        create_dataset(
            datasets_api=datasets_api,
            name="pair-diag",
            dataset_type="train",
            version=f"train-stage-{stage}",
            description=f"training dataset for pair-diag-stage-{stage}, bootstrap dataset",
            datapacks=train_datapacks,
        )

        create_dataset(
            datasets_api=datasets_api,
            name="pair-diag",
            dataset_type="test",
            version=f"test-stage-{stage}",
            description=f"test dataset for pair-diag-stage-{stage}, bootstrap dataset",
            datapacks=test_datapacks,
        )


@app.command()
@timeit()
def cleanup():
    with RCABenchClient(base_url=get_config().base_url) as client:
        datasets_api = DatasetsApi(client)
        resp = datasets_api.api_v2_datasets_get(search="pair-diag")
        assert resp.code is not None and resp.code < 300 and resp.data is not None and resp.data.items is not None
        for dataset in resp.data.items:
            if dataset.id is None:
                continue
            resp = datasets_api.api_v2_datasets_id_delete(
                id=dataset.id,
            )
            logger.info(resp)


if __name__ == "__main__":
    app()
