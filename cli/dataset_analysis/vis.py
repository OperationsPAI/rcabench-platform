#!/usr/bin/env -S uv run -s
# Configure matplotlib backend early for multi-processing safety
from dataclasses import dataclass
from datetime import datetime

import matplotlib
import polars as pl
from dotenv import load_dotenv

from rcabench_platform.v2.datasets.rcabench import valid
from rcabench_platform.v2.utils.fmap import fmap_processpool

matplotlib.use("Agg")
import functools
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from rcabench.openapi import (
    ApiClient,
    DatasetsApi,
    DtoDatapackDetectorReq,
    DtoDetectorRecord,
    EvaluationApi,
    HandlerNode,
)

from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.datasets.train_ticket import extract_path

from .dataset_analysis import Analyzer

load_dotenv()

DEFAULT_NAMESPACE = "ts"


@dataclass
class BarChartMeta:
    """
    Metadata for chart visualization.
    """

    x: list[str]
    y: list[int]
    x_label: str
    y_label: str
    title: str
    save_path: Path

    def __post_init__(self):
        if not self.save_path.parent.exists():
            self.save_path.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class HeatMapMeta:
    """
    Metadata for heatmap visualization.
    """

    x: list[str]
    y: list[str]
    x_label: str
    y_label: str
    title: str
    matrix: np.ndarray
    save_path: Path

    def __post_init__(self):
        if not isinstance(self.matrix, np.ndarray):
            raise ValueError("Matrix must be a numpy ndarray")
        if not self.save_path.parent.exists():
            self.save_path.parent.mkdir(parents=True, exist_ok=True)


@staticmethod
def get_timestamp() -> str:
    """Generate timestamp in YYYY-MM-DD_HH-MM-SS format"""
    time_format = "%Y-%m-%d_%H-%M-%S"
    return datetime.now().strftime(time_format)


# ================== Dataset Results Visualization ==================
class VisDataset:
    def __init__(self, client: ApiClient, dataset_id: int, nodes: list[HandlerNode]):
        """
        Initialize the visualization with a list of datapacks.
        """

        self.analyzer: Analyzer = Analyzer(client=client, namespace=DEFAULT_NAMESPACE, nodes=nodes)
        self.nodes: list[HandlerNode] = nodes
        self.output_path = Path("temp") / "vis_dataset" / f"dataset_{dataset_id}" / get_timestamp()
        if not self.output_path.exists():
            self.output_path.mkdir(parents=True, exist_ok=True)

        self.distribution = self.analyzer.get_distribution()

    @staticmethod
    def _plot_bar_chart(meta: BarChartMeta):
        plt.figure(figsize=(12, 6))
        bars = plt.bar(range(len(meta.x)), meta.y, color="skyblue", edgecolor="navy", alpha=0.7)

        plt.xlabel(meta.x_label)
        plt.ylabel(meta.y_label)
        plt.title(meta.title)
        plt.xticks(range(len(meta.x)), [str(f) for f in meta.x], rotation=45)

        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2.0, height + 0.1, f"{int(height)}", ha="center", va="bottom")

        plt.tight_layout()

        plt.savefig(meta.save_path, dpi=300, bbox_inches="tight")
        logger.info(f"chart saved to: {meta.save_path}")

    @staticmethod
    def _plot_heatmap(meta: HeatMapMeta) -> None:
        plt.figure(figsize=(max(12, len(meta.x)), max(8, len(meta.y) * 0.5)))

        im = plt.imshow(meta.matrix, cmap="YlOrRd", aspect="auto")

        plt.xticks(range(len(meta.x)), [str(f) for f in meta.x], rotation=45)
        plt.yticks(range(len(meta.y)), meta.y)

        plt.xlabel("Fault Type")
        plt.ylabel("Service Name")
        plt.title("Fault-Service Injection Heatmap")

        cbar = plt.colorbar(im)
        cbar.set_label("Injection Count")

        for i in range(len(meta.y)):
            for j in range(len(meta.x)):
                value = meta.matrix[i, j]
                if value == int(value):
                    display_text = f"{int(value)}"
                else:
                    display_text = f"{value:.4f}"

                if meta.matrix[i, j] > 0:
                    plt.text(
                        j,
                        i,
                        display_text,
                        ha="center",
                        va="center",
                        color="white" if meta.matrix[i, j] > meta.matrix.max() / 2 else "black",
                    )

        plt.tight_layout()

        plt.savefig(meta.save_path, dpi=300, bbox_inches="tight")
        logger.info(f"chart saved to: {meta.save_path}")

    def _display_fault_distribution(self) -> None:
        """
        Display fault type distribution.
        """
        if not self.distribution.faults:
            logger.warning("No fault data available for plotting")
            return

        faults, counts = zip(*self.distribution.faults.items())
        self._plot_bar_chart(
            BarChartMeta(
                x=list(faults),
                y=list(counts),
                x_label="Fault Type",
                y_label="Injection Count",
                title="Fault Distribution",
                save_path=self.output_path / "fault_distribution.png",
            )
        )

    def _display_service_distribution(self) -> None:
        """
        Display service name distribution.
        """
        if not self.distribution.services:
            logger.warning("No service data available for plotting")
            return

        services, counts = zip(*self.distribution.services.items())
        self._plot_bar_chart(
            BarChartMeta(
                x=list(services),
                y=list(counts),
                x_label="Service Name",
                y_label="Injection Count",
                title="Service Distribution",
                save_path=self.output_path / "service_distribution.png",
            )
        )

    def _display_fault_service_distribution(self) -> None:
        """
        Display fault-service pair distribution.
        """
        if not self.distribution.pairs:
            logger.warning("No fault_service data available for plotting")
            return

        faults = list(self.distribution.fault_services.keys())
        service_set: set[str] = set()
        for service_count_mapping in self.distribution.fault_services.values():
            service_set.update(service_count_mapping.keys())

        services = list(service_set)

        matrix = np.zeros((len(services), len(faults)))
        for fault, service_count_mapping in self.distribution.fault_services.items():
            fault_idx = faults.index(fault)
            for service, count in service_count_mapping.items():
                service_idx = services.index(service)
                matrix[service_idx, fault_idx] = count

        self._plot_heatmap(
            HeatMapMeta(
                x=faults,
                y=services,
                x_label="Fault Type",
                y_label="Service Name",
                title="Fault-Service Injection Heatmap",
                matrix=matrix,
                save_path=self.output_path / "fault_service_distribution.png",
            )
        )

    def _display_fault_pair_attribute_distribution(self) -> None:
        """
        Display fault-pair attribute coverage distribution.
        """
        if not self.distribution.fault_pair_attribute_coverages:
            logger.warning("No fault-pair attribute coverage data available for plotting")
            return

        faults = list(self.distribution.fault_pair_attribute_coverages.keys())
        pair_set: set[str] = set()
        for pair_ratio_mapping in self.distribution.fault_pair_attribute_coverages.values():
            pair_set.update(pair_ratio_mapping.keys())

        pairs = list(pair_set)

        matrix = np.zeros((len(pairs), len(faults)))
        for fault, pair_ratio_mapping in self.distribution.fault_pair_attribute_coverages.items():
            fault_idx = faults.index(fault)
            for pair, ratio in pair_ratio_mapping.items():
                pair_idx = pairs.index(pair)
                matrix[pair_idx, fault_idx] = ratio

        self._plot_heatmap(
            HeatMapMeta(
                x=faults,
                y=pairs,
                x_label="Fault Type",
                y_label="Pair Name",
                title="Fault-Pair Attribute Coverage Heatmap",
                matrix=matrix,
                save_path=self.output_path / "fault_pair_attribute_coverage_distribution.png",
            )
        )

    def _display_fault_service_attribute_coverage_distribution(self) -> None:
        """
        Display fault-service attribute coverage distribution.
        """
        if not self.distribution.fault_service_attribute_coverages:
            logger.warning("No attribute coverage data available for plotting")
            return

        faults = list(self.distribution.fault_service_attribute_coverages.keys())
        service_set: set[str] = set()
        for service_ratio_mapping in self.distribution.fault_service_attribute_coverages.values():
            service_set.update(service_ratio_mapping.keys())

        services = list(service_set)

        matrix = np.zeros((len(services), len(faults)))
        for fault, service_ratio_mapping in self.distribution.fault_service_attribute_coverages.items():
            fault_idx = faults.index(fault)
            for service, ratio in service_ratio_mapping.items():
                service_idx = services.index(service)
                matrix[service_idx, fault_idx] = ratio

        self._plot_heatmap(
            HeatMapMeta(
                x=faults,
                y=services,
                x_label="Fault Type",
                y_label="Service Name",
                title="Fault-Service Attribute Coverage Heatmap",
                matrix=matrix,
                save_path=self.output_path / "fault_service_attribute_coverage_distribution.png",
            )
        )

    def vis_call(self) -> None:
        """
        Display all distributions.
        """
        self._display_fault_distribution()
        self._display_service_distribution()
        self._display_fault_service_distribution()
        self._display_fault_pair_attribute_distribution()
        self._display_fault_service_attribute_coverage_distribution()


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


@app.command(name="vis-dataset")
def visualize_dataset(dataset_id: int = 5) -> None:
    with RCABenchClient() as client:
        datasets_api = DatasetsApi(client)
        resp = datasets_api.api_v2_datasets_id_get(id=dataset_id, include_injections=True, include_labels=True)
        assert resp.data is not None and resp.data.injections is not None, "No injections found"

        nodes: list[HandlerNode] = []
        for injection in resp.data.injections:
            if injection.engine_config:
                nodes.append(HandlerNode.from_json(injection.engine_config))

        processor = VisDataset(client=client, dataset_id=dataset_id, nodes=nodes)
        processor.vis_call()


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

    fmap_processpool(tasks, parallel=32, cpu_limit_each=2)


if __name__ == "__main__":
    app()
