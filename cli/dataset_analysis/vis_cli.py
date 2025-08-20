#!/usr/bin/env -S uv run -s
# Configure matplotlib backend early for multi-processing safety
import os
import sys
import traceback
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv
from rcabench.openapi import (
    ApiClient,
    DatasetsApi,
    DtoInjectionV2Response,
    DtoInjectionV2SearchReq,
    InjectionsApi,
    ProjectsApi,
)

from cli.dataset_analysis.dataset_analysis import Analyzer, Distribution
from cli.dataset_analysis.vis.injections import VisInjections
from rcabench_platform.v2.analysis.detector_visualization import batch_visualization, get_timestamp
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.datasets.rcabench import valid

DEFAULT_NAMESPACE = "ts"
ALGORITHMS = ["baro", "simplerca", "microdig", "traceback"]
DEGREES = ["absolute_anomaly", "may_anomaly", "no_anomaly"]
METRICS = ["SDD@1", "CPL", "RootServiceDegree"]


load_dotenv()


@app.command()
def manual_vis_detector(datapacks: list[Path], skip_existing: bool = False) -> None:
    batch_visualization(datapacks, skip_existing)


@app.command()
def auto_vis_detector(skip_existing: bool = True) -> None:
    datapack_path = Path("data") / "rcabench_dataset"
    if not datapack_path.exists():
        logger.error(f"Datapack directory not found: {datapack_path}")
        return

    valid_datapacks = []
    for p in datapack_path.iterdir():
        if p.is_dir() and valid(datapack_path / p.name):
            valid_datapacks.append(p)

    batch_visualization(valid_datapacks, skip_existing=skip_existing)


def prepare_injections_data(
    client: ApiClient, dataset_id: int | None = None, project_id: int | None = None
) -> tuple[dict[str, list[DtoInjectionV2Response]], Path]:
    def _get_injections() -> tuple[dict[str, list[DtoInjectionV2Response]], Path]:
        folder_name = "injections"
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

        return injections_dict, Path(folder_name) / get_timestamp()

    def _get_injections_by_id() -> tuple[list[DtoInjectionV2Response], Path]:
        if dataset_id is not None:
            folder_name = f"dataset_{dataset_id}"
            api = DatasetsApi(client)
            resp = api.api_v2_datasets_id_get(id=dataset_id, include_injections=True)

            if not resp or not resp.data or not resp.data.injections:
                raise ValueError(f"No injections found for dataset {dataset_id}")

            return resp.data.injections, Path(folder_name) / get_timestamp()

        elif project_id is not None:
            folder_name = f"project_{project_id}"
            api = ProjectsApi(client)
            resp = api.api_v2_projects_id_get(id=project_id, include_injections=True)

            if not resp or not resp.data or not resp.data.injections:
                raise ValueError(f"No injections found for project {project_id}")

            return resp.data.injections, Path(folder_name) / get_timestamp()

        else:
            raise ValueError("Either dataset_id or project_id must be provided")

    def _filter_injections(injections: list[DtoInjectionV2Response]) -> dict[str, list[DtoInjectionV2Response]]:
        items_dict: dict[str, list[DtoInjectionV2Response]] = dict([(degree, []) for degree in DEGREES])
        for injection in injections:
            if injection.labels is not None:
                for label in injection.labels:
                    if label.value is not None and label.value in items_dict:
                        items_dict[label.value].append(injection)

        return items_dict

    if dataset_id is not None or project_id is not None:
        injections, folder_path = _get_injections_by_id()
        injections_dict = _filter_injections(injections)
        return injections_dict, folder_path
    else:
        return _get_injections()


@app.command(name="vis-injection")
def visualize_injecion(dataset_id: int | None = None, project_id: int | None = None) -> None:
    withDatasetID = dataset_id is not None
    withProjectID = project_id is not None

    if withDatasetID and withProjectID:
        logger.error("Please provide either dataset_id or project_id, not both.")
        return

    injections_dict: dict[str, list[DtoInjectionV2Response]] = {}
    distributions: dict[str, Distribution] = {}

    with RCABenchClient() as client:
        try:
            injections_dict, folder_path = prepare_injections_data(
                client=client, dataset_id=dataset_id, project_id=project_id
            )

            for degree, injections in injections_dict.items():
                analyzer = Analyzer(
                    client=client,
                    namespace=DEFAULT_NAMESPACE,
                    metrics=METRICS,
                    algorithms=ALGORITHMS,
                    injections=injections,
                )
                distributions[degree] = analyzer.get_distribution()

        except ValueError as e:
            traceback.print_exc()
            logger.error(f"Error fetching injections: {e}")
            return

    if not distributions:
        logger.warning("No valid distributions found for visualization")
        return

    distributions_dict: dict[str, dict[str, Any]] = {}
    for degree, distribution in distributions.items():
        if not distribution:
            logger.warning(f"No valid bars found for degree {degree}")
            continue

        distributions_dict[degree] = distribution.to_dict()

    processor = VisInjections(distributions_dict=distributions_dict, metrics=METRICS)

    bars = processor.display_bars()
    if not bars:
        logger.warning("No valid bars found for visualization")
        return

    if not isinstance(bars["services"], dict):
        bars["services"].save(Path("temp") / "fault_bar.png")


if __name__ == "__main__":
    app()
