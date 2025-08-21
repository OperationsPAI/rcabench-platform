from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

import polars as pl
from rcabench.openapi import HandlerNode

from ..logging import logger
from .data_prepare import Item, get_conf

MIN_DEPTH_FOR_RANGE = 2


@dataclass
class CoverageItem:
    fault_type: str
    injected_service: str
    is_pair: bool
    range_num: int = 0
    attribute_covers: dict[str, bool] = field(default_factory=dict)

    @property
    def coverage(self) -> float:
        return len(self.attribute_covers) / self.range_num if self.range_num > 0 else 0.0

    @property
    def key(self) -> str:
        return f"{self.fault_type}-{self.injected_service}"


@dataclass
class PairStats:
    in_degree: int = 0
    out_degree: int = 0


@dataclass
class Distribution:
    faults: dict[str, int] = field(default_factory=dict)
    services: dict[str, int] = field(default_factory=dict)
    pairs: dict[str, PairStats] = field(default_factory=dict)

    fault_services: dict[str, dict[str, int]] = field(default_factory=dict)
    fault_service_attribute_coverages: dict[str, dict[str, float]] = field(default_factory=dict)
    fault_service_metrics: dict[str, dict[str, dict[str, dict[str, int]]]] = field(default_factory=dict)

    fault_pairs: dict[str, dict[str, int]] = field(default_factory=dict)
    fault_pair_attribute_coverages: dict[str, dict[str, float]] = field(default_factory=dict)
    fault_pair_metrics: dict[str, dict[str, dict[str, dict[str, int]]]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "faults": self.faults,
            "services": self.services,
            "pairs": {k: asdict(v) for k, v in self.pairs.items()},
            "fault_services": self.fault_services,
            "fault_service_attribute_coverages": self.fault_service_attribute_coverages,
            "fault_service_metrics": {
                k: {sk: sv for sk, sv in v.items()} for k, v in self.fault_service_metrics.items()
            },
            "fault_pairs": self.fault_pairs,
            "fault_pair_attribute_coverages": self.fault_pair_attribute_coverages,
            "fault_pair_metrics": {k: {sk: sv for sk, sv in v.items()} for k, v in self.fault_pair_metrics.items()},
        }


def get_coverage_items(count_items: list[Item], reference: HandlerNode) -> list[CoverageItem]:
    """
    Generate coverage items from count items and reference node.

    Args:
        count_items: List of items to process for coverage calculation
        reference: Reference handler node containing the full configuration tree

    Returns:
        List of coverage items with calculated ranges and attribute covers

    Raises:
        ValueError: If reference node has no children
    """

    class RangeProcessor:
        default_value = 0

        @staticmethod
        def __call__(node: HandlerNode, **kwargs) -> int:
            return node.range[1] - node.range[0] + 1 if node.range else 0

        @staticmethod
        def combine(results: list[int]) -> int:
            return sum(results)

    class CoverageProcessor:
        default_value = {}

        @staticmethod
        def __call__(node: HandlerNode, **kwargs) -> dict[str, bool]:
            return {f"{kwargs['key']}-{node.value}": True}

        @staticmethod
        def combine(results: list[dict]) -> dict[str, bool]:
            combined = {}
            for result in results:
                combined.update(result)
            return combined

    def _traverse_node(node: HandlerNode, key: str, processor: RangeProcessor | CoverageProcessor) -> Any:
        int_key = int(key)

        if node.children is None:
            return processor(node, key=key) if int_key > MIN_DEPTH_FOR_RANGE else processor.default_value

        results = []
        for child_key, child_node in node.children.items():
            results.append(_traverse_node(child_node, child_key, processor))

        return processor.combine(results)

    if reference.children is None:
        raise ValueError("Reference node must have children to calculate coverage items")

    fault_range_mapping: dict[str, int] = {
        key: _traverse_node(node, key, RangeProcessor()) for key, node in reference.children.items()
    }

    coverage_item_dict: dict[str, CoverageItem] = {}
    for count_item in count_items:
        key = f"{count_item.fault_type}-{count_item.injected_service}"
        attribute_covers = _traverse_node(count_item.node, str(count_item.node.value), CoverageProcessor())

        if key not in coverage_item_dict:
            coverage_item_dict[key] = CoverageItem(
                fault_type=count_item.fault_type,
                injected_service=count_item.injected_service,
                is_pair="->" in count_item.injected_service,
                range_num=fault_range_mapping[str(count_item.node.value)],
                attribute_covers=attribute_covers,
            )
        else:
            coverage_item_dict[key].attribute_covers.update(attribute_covers)

    return list(coverage_item_dict.values())


def get_datapacks_distribution(count_items: list[Item], metrics: list[str], namespace: str) -> Distribution:
    """
    Generate comprehensive distribution analysis from count items.

    Args:
        count_items: List of items to analyze
        reference: Optional reference handler node for coverage analysis

    Returns:
        Distribution object containing all calculated distributions and coverages
    """

    distribution = Distribution()

    # Basic distributions
    distribution.faults = calculate_faults_distribution(count_items)
    distribution.services = calculate_services_distribution(count_items)
    distribution.pairs = calculate_pairs_distribution(count_items)

    # Composite distributions
    distribution.fault_services = calculate_fault_services_distribution(count_items)
    distribution.fault_pairs = calculate_fault_pairs_distribution(count_items)

    # Coverage analysis
    reference = get_conf(namespace)
    coverage_items = get_coverage_items(count_items, reference)
    distribution.fault_service_attribute_coverages = calculate_fault_service_attribute_coverages(coverage_items)
    distribution.fault_pair_attribute_coverages = calculate_fault_pair_attribute_coverages(coverage_items)

    # Datapack Metric distributions
    distribution.fault_service_metrics = calculate_groupby_datapack_metric_distributions(count_items, metrics)

    return distribution


def calculate_distribution(
    items: list[Item], extractor: Callable[[Item], str], filter_func: Callable[[Item], bool] | None = None
) -> dict[str, int]:
    """
    Generic function to calculate distribution of extracted values from items.

    Args:
        items: List of items to process
        extractor: Function to extract value from each item
        filter_func: Optional function to filter items before processing

    Returns:
        Dict mapping extracted values to their counts
    """
    if filter_func:
        items = [item for item in items if filter_func(item)]

    values = [extractor(item) for item in items if extractor(item)]
    return dict(Counter(values))


def calculate_faults_distribution(count_items: list[Item]) -> dict[str, int]:
    """
    Calculate the distribution of fault types.

    Args:
        count_items: List of items to analyze

    Returns:
        Dict mapping fault types to their occurrence counts
    """
    return calculate_distribution(count_items, lambda item: item.fault_type)


def calculate_services_distribution(count_items: list[Item]) -> dict[str, int]:
    """
    Calculate the distribution of injected services.

    Args:
        count_items: List of items to analyze

    Returns:
        Dict mapping service names to their occurrence counts
    """
    return calculate_distribution(count_items, lambda item: item.injected_service)


def calculate_pairs_distribution(count_items: list[Item]) -> dict[str, PairStats]:
    """
    Calculate the distribution of service pairs with in/out degree statistics.

    Args:
        count_items: List of items to analyze

    Returns:
        Dict mapping service names to their pair statistics (in_degree, out_degree)
    """
    pairs_distribution: dict[str, PairStats] = defaultdict(PairStats)

    for item in filter(lambda x: x.is_pair, count_items):
        if "->" not in item.injected_service:
            continue

        source, target = item.injected_service.split("->", 1)
        pairs_distribution[source].out_degree += 1
        pairs_distribution[target].in_degree += 1

    return dict(pairs_distribution)


def calculate_fault_targets_distribution(count_items: list[Item], is_pair: bool) -> dict[str, dict[str, int]]:
    """
    Generic function to calculate the distribution of fault-target combinations.

    Args:
        count_items: List of items to analyze
        is_pair: If True, analyze pairs; if False, analyze individual services

    Returns:
        Dict mapping fault types to dict of targets and their counts
    """
    fault_targets = defaultdict(list)

    for item in count_items:
        if item.fault_type and item.injected_service and item.is_pair == is_pair:
            fault_targets[item.fault_type].append(item.injected_service)

    return {fault_type: dict(Counter(targets)) for fault_type, targets in fault_targets.items()}


def calculate_fault_services_distribution(count_items: list[Item]) -> dict[str, dict[str, int]]:
    """
    Calculate the distribution of fault-service combinations.

    Args:
        count_items: List of items to analyze

    Returns:
        Dict mapping fault types to dict of services and their counts
    """
    return calculate_fault_targets_distribution(count_items, is_pair=False)


def calculate_fault_pairs_distribution(count_items: list[Item]) -> dict[str, dict[str, int]]:
    """
    Calculate the distribution of fault-pair combinations.

    Args:
        count_items: List of items to analyze

    Returns:
        Dict mapping fault types to dict of service pairs and their counts
    """
    return calculate_fault_targets_distribution(count_items, is_pair=True)


def calculate_coverage_by_type(coverage_items: list[CoverageItem], is_pair: bool) -> dict[str, dict[str, float]]:
    """
    Generic function to calculate coverage by target type.

    Args:
        coverage_items: List of coverage items to analyze
        is_pair: If True, analyze pairs; if False, analyze individual services

    Returns:
        Dict mapping fault types to dict of targets and their coverage percentages
    """
    result = defaultdict(dict)

    for item in coverage_items:
        if item.is_pair == is_pair:
            result[item.fault_type][item.injected_service] = item.coverage

    return dict(result)


def calculate_fault_service_attribute_coverages(coverage_items: list[CoverageItem]) -> dict[str, dict[str, float]]:
    """
    Calculate fault-service attribute coverage percentages.

    Args:
        coverage_items: List of coverage items to analyze

    Returns:
        Dict mapping fault types to dict of services and their coverage percentages
    """
    return calculate_coverage_by_type(coverage_items, is_pair=False)


def calculate_fault_pair_attribute_coverages(coverage_items: list[CoverageItem]) -> dict[str, dict[str, float]]:
    """
    Calculate fault-pair attribute coverage percentages.

    Args:
        coverage_items: List of coverage items to analyze

    Returns:
        Dict mapping fault types to dict of service pairs and their coverage percentages
    """
    return calculate_coverage_by_type(coverage_items, is_pair=True)


def calculate_groupby_datapack_metric_distributions(
    count_items: list[Item], metrics: list[str]
) -> dict[str, dict[str, dict[str, dict[str, int]]]]:
    """
    Calculate the distributions of fault-service metrics grouped by fault type and service.

    Args:
        count_items: List of items containing metric data
        metrics: List of metric names to analyze

    Returns:
        Dict mapping fault types to dict of services to dict of metrics to dict of values and counts
    """
    fault_service_metrics: dict[str, dict[str, dict[str, dict[str, int]]]] = {}

    data: list[dict[str, Any]] = []
    for count_item in count_items:
        if count_item.is_pair:
            for key, value in count_item.datapack_metric_values.items():
                data.append(
                    {
                        "fault_type": count_item.fault_type,
                        "service": count_item.injected_service,
                        "metric_name": key,
                        "metric_value": value,
                    }
                )

    if not data:
        return {}

    lf = pl.LazyFrame(data=data)
    metrics_filter = pl.col("metric_name").is_in(metrics)
    filtered_lf = lf.filter(metrics_filter)

    collected_data = filtered_lf.collect()
    if collected_data.is_empty():
        return {}

    for (fault_type_object, service_object), group_df in collected_data.group_by(["fault_type", "service"]):
        fault_type = str(fault_type_object)
        service = str(service_object)

        if fault_type not in fault_service_metrics:
            fault_service_metrics[fault_type] = {}

        if service not in fault_service_metrics[fault_type]:
            fault_service_metrics[fault_type][service] = {}

        for metric in metrics:
            metric_data = group_df.filter(pl.col("metric_name") == metric)

            if metric_data.is_empty():
                continue

            distribution_stats = metric_data.group_by("metric_value").agg(pl.len().alias("count")).sort("metric_value")

            distribution_dict = {
                str(row["metric_value"]): row["count"] for row in distribution_stats.iter_rows(named=True)
            }

            fault_service_metrics[fault_type][service][metric] = distribution_dict

    return fault_service_metrics
