#!/usr/bin/env -S uv run -s
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from rcabench.openapi import (
    ApiClient,
    DatasetsApi,
    DtoAlgorithmDatapackReq,
    DtoAlgorithmDatapackResp,
    DtoDatapackEvaluationBatchReq,
    DtoGranularityRecord,
    DtoInjectionV2Response,
    DtoInjectionV2SearchReq,
    EvaluationApi,
    InjectionsApi,
    ProjectsApi,
)

from rcabench_platform.v2.analysis.data_prepare import InputItem, build_items_with_cache
from rcabench_platform.v2.analysis.datapacks_analysis import (
    Distribution,
    get_datapacks_distribution,
)
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient

DEFAULT_NAMESPACE = "ts"
ALGORITHMS = ["baro", "simplerca", "microdig", "traceback"]
DEGREES = ["absolute_anomaly", "may_anomaly", "no_anomaly"]
METRICS = ["SDD@1", "CPL", "RootServiceDegree"]


load_dotenv()


def prepare_injections_data(
    client: ApiClient, dataset_id: int | None = None, project_id: int | None = None
) -> tuple[dict[str, list[DtoInjectionV2Response]], str]:
    def _get_injections() -> tuple[dict[str, list[DtoInjectionV2Response]], str]:
        api = InjectionsApi(client)

        injections_dict: dict[str, list[DtoInjectionV2Response]] = {}
        for degree in DEGREES:
            resp = api.api_v2_injections_search_post(
                search=DtoInjectionV2SearchReq(
                    tags=[degree],
                    include_labels=True,
                )
            )
            if not resp or not resp.data or not resp.data.items:
                raise ValueError(f"No injections found for degree {degree}")

            injections_dict[degree] = resp.data.items

        return injections_dict, "injections"

    def _get_injections_by_id() -> tuple[dict[str, list[DtoInjectionV2Response]], str]:
        injections: list[DtoInjectionV2Response] = []
        folder_name = ""

        if dataset_id is not None:
            api = DatasetsApi(client)
            resp = api.api_v2_datasets_id_get(id=dataset_id, include_injections=True)

            if not resp or not resp.data or not resp.data.injections:
                raise ValueError(f"No injections found for dataset {dataset_id}")

            injections = resp.data.injections
            folder_name = f"dataset_{dataset_id}"

        elif project_id is not None:
            api = ProjectsApi(client)
            resp = api.api_v2_projects_id_get(id=project_id, include_injections=True)

            if not resp or not resp.data or not resp.data.injections:
                raise ValueError(f"No injections found for project {project_id}")

            injections = resp.data.injections
            folder_name = f"project_{dataset_id}"

        else:
            raise ValueError("Either dataset_id or project_id must be provided")

        items_dict: dict[str, list[DtoInjectionV2Response]] = dict([(degree, []) for degree in DEGREES])
        for injection in injections:
            if injection.labels is not None:
                for label in injection.labels:
                    if label.value is not None and label.value in items_dict:
                        items_dict[label.value].append(injection)

        return items_dict, folder_name

    if dataset_id is not None or project_id is not None:
        return _get_injections_by_id()
    else:
        return _get_injections()


@app.command(name="visualize")
def visualize(dataset_id: int | None = None, project_id: int | None = None) -> None:
    withDatasetID = dataset_id is not None
    withProjectID = project_id is not None

    if withDatasetID and withProjectID:
        logger.error("Please provide either dataset_id or project_id, not both.")
        return

    injections_dict: dict[str, list[DtoInjectionV2Response]] = {}
    distributions: dict[str, Distribution] = {}

    with RCABenchClient() as client:
        evaluator = EvaluationApi(client)

        injections_dict, folder_name = prepare_injections_data(
            client=client, dataset_id=dataset_id, project_id=project_id
        )

        for degree, injections in injections_dict.items():
            prefix = f"{degree}_"
            algo_evals: dict[str, list[DtoGranularityRecord]] = {}

            ori_df = pl.DataFrame(
                data=[
                    {"algorithm": algorithm, "datapack": injection.injection_name}
                    for algorithm in ALGORITHMS
                    for injection in injections
                ]
            )

            resp = evaluator.api_v2_evaluations_datapacks_post(
                request=DtoDatapackEvaluationBatchReq(
                    items=[
                        DtoAlgorithmDatapackReq(
                            algorithm=algorithm,
                            datapack=datapack,
                        )
                        for algorithm, datapack in ori_df.iter_rows()
                    ]
                )
            )

            assert resp.data is not None, "Failed to get evaluation data"
            eval_df = pl.DataFrame(data=resp.data)

            joined_df = ori_df.join(
                eval_df, left_on=["algorithm", "datapack"], right_on=["algorithm", "datapack"], how="inner"
            )

            input_items: list[InputItem] = []
            injections_mapping: dict[str, DtoInjectionV2Response] = {
                injection.injection_name: injection for injection in injections if injection.injection_name is not None
            }

            for keys, group_df in joined_df.group_by("datapack"):
                datapack = str(keys[0])
                injection = injections_mapping.get(datapack)
                if injection is None:
                    logger.warning(f"No injection found for datapack {datapack}")
                    continue

                algo_evals: dict[str, list[DtoGranularityRecord]] = {}

                for row in group_df.iter_rows(named=True):
                    algorithm: str = row["algorithm"]
                    predictions: list[dict[str, Any]] = row.get("predictions", [])
                    if not predictions:
                        logger.warning(f"No predictions found for algorithm {algorithm} and datapack {datapack}")
                        continue

                    algo_evals[algorithm] = [DtoGranularityRecord.from_dict(p) for p in predictions]

                input_items.append(
                    InputItem(
                        algo_evals=algo_evals if algo_evals else None,
                        injection=injection,
                    )
                )

            count_items = build_items_with_cache(
                output_pkl_path=Path("temp/dataset_analysis/datapacks") / folder_name / f"{prefix}items.pkl",
                input_items=input_items,
                metrics=METRICS,
                namespace=DEFAULT_NAMESPACE,
            )
            distributions[degree] = get_datapacks_distribution(
                count_items=count_items, metrics=METRICS, namespace=DEFAULT_NAMESPACE
            )

    if not distributions:
        logger.warning("No valid distributions found for visualization")
        return

    distributions_dict: dict[str, dict[str, Any]] = {}
    for degree, distribution in distributions.items():
        if not distribution:
            logger.warning(f"No valid bars found for degree {degree}")
            continue

        distributions_dict[degree] = distribution.to_dict()


if __name__ == "__main__":
    app()
