#!/usr/bin/env -S uv run -s
# Configure matplotlib backend early for multi-processing safety
import functools
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl
from dotenv import load_dotenv
from rcabench.openapi import (
    ApiClient,
    DatasetsApi,
    DtoDatapackDetectorReq,
    DtoDetectorRecord,
    DtoInjectionV2Response,
    DtoInjectionV2SearchReq,
    EvaluationApi,
    InjectionsApi,
    ProjectsApi,
)

from cli.dataset_analysis.dataset_analysis import Analyzer, Distribution
from cli.dataset_analysis.vis.injections import VisInjections
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.datasets.rcabench import valid
from rcabench_platform.v2.datasets.train_ticket import extract_path
from rcabench_platform.v2.utils.fmap import fmap_processpool

DEFAULT_NAMESPACE = "ts"

ALGORITHMS = ["baro", "simplerca", "microdig", "traceback"]
DEGREES = ["absolute_anomaly", "may_anomaly", "no_anomaly"]
METRICS = ["SDD@1", "CPL", "RootServiceDegree"]


load_dotenv()


# ================== Detector Results Visualization ==================
class VisDetector:
    """
    Class for visualizing detector results.
    """

    def __init__(self, datapack: Path):
        self.datapack: Path = datapack
        self.output_path: Path = Path("temp") / "vis_detector" / get_timestamp()

    @staticmethod
    def _extract_status_code(span_attributes: str) -> str:
        """
        Extract HTTP status code from span attributes.
        """
        try:
            ra = json.loads(span_attributes) if span_attributes else {}
            return ra["http.status_code"]
        except Exception:
            return "-1"

    # ================== Data Preparation Functions ==================

    def _prepare_trace_data(self) -> None:
        """
        Load and prepare trace data for visualization.
        """
        df1: pl.DataFrame = pl.scan_parquet(self.datapack / "normal_traces.parquet").collect()
        df2: pl.DataFrame = pl.scan_parquet(self.datapack / "abnormal_traces.parquet").collect()

        self.normal_df = df1.with_columns(pl.lit("normal").alias("trace_type"))
        self.abnormal_df = df2.with_columns(pl.lit("abnormal").alias("trace_type"))
        self.start_time = df1.select(pl.col("Timestamp").min()).item()
        self.last_normal_time = df1.select(pl.col("Timestamp").max()).item()

    def _prepare_entry_data(self) -> None:
        """
        Prepare entry point data from trace data.
        """
        merged_df: pl.DataFrame = pl.concat([self.normal_df, self.abnormal_df])
        entry_df: pl.DataFrame = merged_df.filter(
            (pl.col("ServiceName") == "loadgenerator")
            & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
        )

        if len(entry_df) == 0:
            logger.error("loadgenerator not found in trace data, using ts-ui-dashboard as fallback")
            entry_df = merged_df.filter(
                (pl.col("ServiceName") == "ts-ui-dashboard")
                & (pl.col("ParentSpanId").is_null() | (pl.col("ParentSpanId") == ""))
            )

        if len(entry_df) == 0:
            logger.error("No valid entrypoint found in trace data")
            self.entry_df = pl.DataFrame()
            return

        entry_df = entry_df.with_columns(
            [
                pl.col("Timestamp").alias("datetime"),
                (pl.col("Duration") / 1e9).alias("duration"),
                pl.struct(["SpanAttributes", "StatusCode"])
                .map_elements(lambda x: self._extract_status_code(x["SpanAttributes"]), return_dtype=pl.Utf8)
                .alias("status_code"),
            ]
        ).sort("Timestamp")

        self.entry_df = entry_df.with_columns(
            pl.col("SpanName").map_elements(extract_path, return_dtype=pl.Utf8).alias("api_path")
        )

    def _create_span_visualization(self) -> None:
        """
        Create visualizations for problematic spans.
        """
        problematic_spans = set()
        for record in self.issue_data:
            problematic_spans.add(record.span_name)

        if not problematic_spans:
            logger.info(f"No specific problematic spans found in {self.datapack.name}")
            return

        # Create figure with subplots - 2 columns for each span (latency and status code)
        _, axes = plt.subplots(len(problematic_spans), 2, figsize=(20, 6 * len(problematic_spans)), dpi=300)
        if len(problematic_spans) == 1:
            axes = axes.reshape(1, -1)

        # Plot each problematic span
        for idx, span_name in enumerate(problematic_spans):
            ax_latency = axes[idx, 0]
            ax_status = axes[idx, 1]

            span_data = self.entry_df.filter(pl.col("api_path") == span_name)

            if len(span_data) == 0:
                for ax in [ax_latency, ax_status]:
                    ax.text(
                        0.5,
                        0.5,
                        f"No data found for {span_name}",
                        ha="center",
                        va="center",
                        transform=ax.transAxes,
                    )
                ax_latency.set_title(f"{span_name} - Latency")
                ax_status.set_title(f"{span_name} - Status Code")
                continue

            # Separate normal and abnormal data
            normal_data = span_data.filter(pl.col("trace_type") == "normal")
            abnormal_data = span_data.filter(pl.col("trace_type") == "abnormal")

            normal_count = len(normal_data)
            abnormal_count = len(abnormal_data)

            # Plot latency over time
            if len(normal_data) > 0:
                # Sort normal data by datetime for proper line connection
                normal_sorted = normal_data.sort("datetime")
                normal_times = normal_sorted.select("datetime").to_numpy().flatten()
                normal_latencies = normal_sorted.select("duration").to_numpy().flatten()
                ax_latency.plot(
                    normal_times,
                    normal_latencies,
                    color="green",
                    alpha=0.7,
                    linewidth=1.2,
                    marker="o",
                    markersize=3,
                    label=f"Normal ({normal_count})",
                )

            if len(abnormal_data) > 0:
                # Sort abnormal data by datetime for proper line connection
                abnormal_sorted = abnormal_data.sort("datetime")
                abnormal_times = abnormal_sorted.select("datetime").to_numpy().flatten()
                abnormal_latencies = abnormal_sorted.select("duration").to_numpy().flatten()
                ax_latency.plot(
                    abnormal_times,
                    abnormal_latencies,
                    color="red",
                    alpha=0.7,
                    linewidth=1.2,
                    marker="o",
                    markersize=3,
                    label=f"Abnormal ({abnormal_count})",
                )

            # Add vertical line at last normal time
            ax_latency.axvline(
                x=self.last_normal_time, color="blue", linestyle="--", alpha=0.7, label="Last Normal Time"
            )

            ax_latency.set_xlabel("Time")
            ax_latency.set_ylabel("Latency (seconds)")
            ax_latency.set_title(f"{span_name} - Latency\n(Normal: {normal_count}, Abnormal: {abnormal_count})")
            ax_latency.legend()
            ax_latency.grid(True, alpha=0.3)

            # Format x-axis for datetime
            ax_latency.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
            ax_latency.tick_params(axis="x", rotation=45)

            # Plot status codes over time
            # Convert status codes to numeric for plotting
            if len(normal_data) > 0:
                # Use sorted data for consistent time ordering
                normal_sorted = normal_data.sort("datetime")
                normal_times = normal_sorted.select("datetime").to_numpy().flatten()
                normal_status = normal_sorted.select("status_code").to_numpy().flatten()
                normal_status_numeric = [int(s) if s.isdigit() else -1 for s in normal_status]
                ax_status.scatter(
                    normal_times,
                    normal_status_numeric,
                    color="green",
                    alpha=0.6,
                    s=10,
                    label=f"Normal ({normal_count})",
                )

            if len(abnormal_data) > 0:
                # Use sorted data for consistent time ordering
                abnormal_sorted = abnormal_data.sort("datetime")
                abnormal_times = abnormal_sorted.select("datetime").to_numpy().flatten()
                abnormal_status = abnormal_sorted.select("status_code").to_numpy().flatten()
                abnormal_status_numeric = [int(s) if s.isdigit() else -1 for s in abnormal_status]
                ax_status.scatter(
                    abnormal_times,
                    abnormal_status_numeric,
                    color="red",
                    alpha=0.6,
                    s=10,
                    label=f"Abnormal ({abnormal_count})",
                )

            # Add vertical line at last normal time
            ax_status.axvline(
                x=self.last_normal_time, color="blue", linestyle="--", alpha=0.7, label="Last Normal Time"
            )

            ax_status.set_xlabel("Time")
            ax_status.set_ylabel("HTTP Status Code")
            ax_status.set_title(f"{span_name} - Status Code\n(Normal: {normal_count}, Abnormal: {abnormal_count})")
            ax_status.legend()
            ax_status.grid(True, alpha=0.3)

            # Format x-axis for datetime
            ax_status.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
            ax_status.tick_params(axis="x", rotation=45)

            # Set y-axis to show common HTTP status codes
            status_codes = [200, 400, 401, 403, 404, 500, 502, 503, 504]
            ax_status.set_yticks(status_codes)

        plt.tight_layout()
        plt.savefig(self.output_file, bbox_inches="tight")
        plt.close()

        logger.info(f"Visualization saved to {self.output_file}")

    def vis_call(self, skip_existing: bool = True) -> None:
        """
        Main method to visualize detector results.
        """
        with RCABenchClient() as client:
            eval_api = EvaluationApi(client)
            resp = eval_api.api_v2_evaluations_datapacks_detector_post(
                request=DtoDatapackDetectorReq(
                    datapacks=[self.datapack.name],
                )
            )
            assert resp.code and resp.code < 300
            assert resp.data is not None and resp.data.items is not None, "No detector results found"
            data: list[DtoDetectorRecord] = [i.results for i in resp.data.items if i.results is not None][0]

        if data is None or len(data) == 0:
            logger.warning(f"No detector results found for {self.datapack.name}, skipping visualization")
            return

        self.issue_data = [i for i in data if i.issue is not None and i.issue != "{}"]
        if len(self.issue_data) == 0:
            logger.info(f"No issues found in {self.datapack.name}, skipping visualization")
            return

        # Prepare trace data
        self._prepare_trace_data()

        if (
            self.normal_df.is_empty()
            or self.abnormal_df.is_empty()
            or self.start_time is None
            or self.last_normal_time is None
        ):
            logger.error(f"Invalid trace data in {self.datapack.name}, skipping visualization")
            return

        hour_key: str = self.start_time.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d_%H")
        final_output_dir: Path = self.output_path / hour_key
        final_output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file: Path = final_output_dir / f"{self.datapack.name}.png"
        if self.output_file.exists() and skip_existing:
            return

        # Prepare entry data
        self._prepare_entry_data()
        if len(self.entry_df) == 0:
            return

        # Create visualization for problematic spans
        self._create_span_visualization()


def process_detector_visualization(datapack_dir: Path, skip_existing: bool) -> None:
    try:
        detector = VisDetector(datapack_dir)
        detector.vis_call(skip_existing=skip_existing)
    except Exception as e:
        logger.error(f"Error processing detector visualization for {datapack_dir}: {e}")


def get_timestamp() -> str:
    """Generate timestamp in YYYY-MM-DD_HH-MM-SS format"""
    time_format = "%Y-%m-%d_%H-%M-%S"
    return datetime.now().strftime(time_format)


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


@app.command(name="vis-detector")
def visualize_detector_results(skip_existing: bool = True) -> None:
    datapack_path = Path("data") / "rcabench_dataset"
    if not datapack_path.exists():
        logger.error(f"Datapack directory not found: {datapack_path}")
        return

    valid_datapacks = []
    for p in datapack_path.iterdir():
        if p.is_dir() and valid(datapack_path / p.name):
            valid_datapacks.append(p)

    tasks = []
    for datapack_dir in valid_datapacks:
        detector = VisDetector(datapack_dir)
        task = functools.partial(detector.vis_call, skip_existing=skip_existing)
        tasks.append(task)

    if tasks:
        fmap_processpool(tasks, parallel=32, cpu_limit_each=2)
    else:
        logger.warning("No valid datapacks found for visualization")


if __name__ == "__main__":
    app()
