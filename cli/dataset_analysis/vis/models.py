from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np


@dataclass
class BarMeta:
    """
    Metadata for chart visualization.
    """

    data: list[dict[str, Any]]
    x_label: str
    y_label: str
    title: str
    save_path: Path | None = None

    def __post_init__(self):
        if self.save_path and not self.save_path.parent.exists():
            self.save_path.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class HeatmapMeta:
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


@dataclass
class PlotDataItem:
    fault: str
    mapping_key: str
    metric_value: float
    count: int
    value_key: str


@dataclass
class LayerData:
    key_datas: list[PlotDataItem]
    x_list: list[int]
    y_list: list[int]
    z_list: list[int]
    sizes: list[float]


@dataclass
class BubbleMeta:
    """
    Metadata for bubble chart visualization.
    """

    layer_datas: dict[str, LayerData]
    x_texts: list[str]
    y_texts: list[str]
    metric: str
    mapping_key_name: str
    save_path: Path


@dataclass
class BarConfig:
    """
    Configuration for common distribution visualization.
    """

    x_label: str
    title: str
    filename: str
    output_csv: bool


@dataclass
class HeatmapConfig:
    """
    Configuration for heatmap chart visualization.
    """

    y_label: str
    title: str
    filename: str


@dataclass
class BubbleConfig:
    """
    Configuration for bubble chart visualization.
    """

    mapping_key_name: str
    filename: str


@dataclass
class VisInjectionsConfig:
    """
    Configuration for visualization of injections.
    """

    bar_configs: dict[str, BarConfig]
    heatmap_configs: dict[str, HeatmapConfig]
    bubble_configs: dict[str, BubbleConfig]


def NewVisInjectionsConfig() -> VisInjectionsConfig:
    return VisInjectionsConfig(
        bar_configs={
            "faults": BarConfig(
                x_label="Fault Type",
                title="Fault Distribution",
                filename="faults",
                output_csv=True,
            ),
            "services": BarConfig(
                x_label="Service Name",
                title="Service Distribution",
                filename="services",
                output_csv=True,
            ),
            "metrics": BarConfig(
                x_label="Metric Value",
                title="{metric} Distribution",
                filename="{metric}",
                output_csv=False,
            ),
        },
        heatmap_configs={
            "fault_services": HeatmapConfig(
                y_label="Service Name",
                title="Fault-Service Count Distribution",
                filename="fault_services",
            ),
            "fault_pair_attribute_coverages": HeatmapConfig(
                y_label="Pair Name",
                title="Fault-Pair Attribute Coverage Distribution",
                filename="fault_pair_attribute_coverages",
            ),
            "fault_service_attribute_coverages": HeatmapConfig(
                y_label="Service Name",
                title="Fault-Service Attribute Coverage Distribution",
                filename="fault_service_attribute_coverages",
            ),
        },
        bubble_configs={
            "fault_pair_metrics": BubbleConfig(
                mapping_key_name="Pair",
                filename="fault_pair_{metric}",
            ),
            "fault_service_metrics": BubbleConfig(
                mapping_key_name="Service",
                filename="fault_service_{metric}",
            ),
        },
    )
