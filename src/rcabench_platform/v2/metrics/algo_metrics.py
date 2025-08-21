import os
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import networkx as nx
import polars as pl
from rcabench.openapi import DtoGranularityRecord

from ..clients.rcabench_ import get_evaluation_by_dataset
from ..datasets.spec import build_service_graph
from ..logging import logger


class AlgoMetrics(TypedDict):
    """Algorithm metrics for a specific granularity level."""

    level: str
    top1: float
    top3: float
    top5: float

    mrr: float

    as1: float
    as3: float
    as5: float

    efficiency: float
    datapack_count: int


@dataclass
class AlgoMetricItem:
    """
    Data class to hold algorithm metric for a specific algorithm and datapack.
    """

    top1: float = 0.0
    top3: float = 0.0
    top5: float = 0.0
    mrr: float = 0.0

    def to_dict(self) -> dict[str, float]:
        """Convert the metric item to a dictionary."""
        return {
            "top1": self.top1,
            "top3": self.top3,
            "top5": self.top5,
            "mrr": self.mrr,
        }


# =============================================================================
# Graph Path Utilities
# =============================================================================


def _get_shortest_path(graph: nx.DiGraph, source: str, target: str) -> list[str]:
    assert source in graph and target in graph, "Source or target not in graph"
    path_result = nx.shortest_path(graph, source=source, target=target)
    return list(path_result) if isinstance(path_result, (list, tuple)) else []


def _as_vertices(path: list[str]) -> set[str]:
    return set(path)


def _cpl(path: list[str]) -> int:
    n = len(path)
    return n - 1 if n > 0 else 0


# =============================================================================
# Path Analysis and Scoring Functions
# =============================================================================


def wcpl(
    path: Iterable[str],
    weights: dict[tuple[str, str], float] | None = None,
) -> float:
    path_list = list(path)
    if len(path_list) <= 1:
        return 0.0

    if weights:
        total_weight = 0.0
        for i in range(len(path_list) - 1):
            edge = (path_list[i], path_list[i + 1])
            edge_weight = weights.get(edge, 1.0)
            total_weight += edge_weight
        return float(total_weight)

    return _cpl(path_list)


def jaccard_score(algo_path: list[str], gt_path: list[str]) -> float:
    va = _as_vertices(algo_path)
    vg = _as_vertices(gt_path)
    union = va | vg
    if not union:
        return 1.0
    inter = va & vg
    return float(len(inter) / len(union))


def effi(algo_path: list[str], gt_path: list[str]) -> float:
    va = _as_vertices(algo_path)
    vg = _as_vertices(gt_path)
    union = va | vg
    if not union:
        return 1.0
    inter = va & vg
    return float(len(inter) / len(va))


def weighted_divergence_distance(
    gt_path: list[str],
    algo_path: list[str],
    divergence_path: list[str],
    weights: dict[tuple[str, str], float] | None = None,
) -> float:
    if weights is None:
        weights = {}

    algo_wcpl = wcpl(algo_path, weights)
    gt_wcpl = wcpl(gt_path, weights)
    div_wcpl = wcpl(divergence_path, weights)

    if gt_wcpl == 0:
        return 0.0

    return (gt_wcpl / (algo_wcpl + div_wcpl)) if (algo_wcpl + div_wcpl) > 0 else 0.0


def single_path_alignment_score(
    algo_path: list[str],
    gt_path: list[str],
    divergence_path: list[str],
    weights: dict[tuple[str, str], float] | None = None,
) -> float:
    """
    AS_sp(P_algo, P_gt) = WDD(P_algo, P_gt) × Jaccard(P_algo, P_gt)
    """
    wdd = weighted_divergence_distance(algo_path, gt_path, divergence_path, weights)
    jaccard = jaccard_score(algo_path, gt_path)
    return wdd * jaccard


def alignment_score_multi_gt(
    algo_path: list[str],
    gt_divergence_path_pairs: list[tuple[list[str], list[str]]],
    weights: dict[tuple[str, str], float] | None = None,
) -> float:
    """Calculate multi-path alignment score.

    AS_mp = max_i AS_sp(P_algo, P_gt^{(i)})

    Args:
        algo_path: Algorithm predicted path
        gt_divergence_path_pairs: List of (ground_truth_path, divergence_path) tuples
        weights: Optional edge weights

    Returns:
        Maximum single-path alignment score across all ground truth paths
    """
    best = 0.0
    for gt, div in gt_divergence_path_pairs:
        score = single_path_alignment_score(algo_path, gt, div, weights=weights)
        if score > best:
            best = score
    return best


# =============================================================================
# Metrics Calculation Functions
# =============================================================================


def calculate_metrics_for_level(
    groundtruth_items: list[str], predictions: list[DtoGranularityRecord], level: str
) -> AlgoMetricItem:
    """Calculate metrics at a specific granularity level.

    Args:
        groundtruth_items: List of groundtruth labels for the granularity level
        predictions: List of algorithm predictions
        level: Name of the granularity level

    Returns:
        Dictionary containing top1, top3, top5, and mrr metrics
    """
    if not groundtruth_items or not predictions:
        return AlgoMetricItem()

    level_predictions = [p for p in predictions if p.level == level]

    if not level_predictions:
        return AlgoMetricItem()

    level_predictions.sort(key=lambda x: x.rank or float("inf"))

    hits = []
    for pred in level_predictions:
        if pred.result in groundtruth_items:
            hits.append(pred.rank or float("inf"))

    if not hits:
        return AlgoMetricItem()

    min_rank = min(hits)
    top1 = 1.0 if min_rank <= 1 else 0.0
    top3 = 1.0 if min_rank <= 3 else 0.0
    top5 = 1.0 if min_rank <= 5 else 0.0

    mrr = 1.0 / min_rank

    return AlgoMetricItem(top1=top1, top3=top3, top5=top5, mrr=mrr)


def calculate_alignment_score(
    datapack_path: Path, entry: str, groundtruth_items: list[str], predictions: list[DtoGranularityRecord], k: int = 5
) -> list[float]:
    normal_traces = pl.scan_parquet(datapack_path / "normal_traces.parquet")
    anomal_traces = pl.scan_parquet(datapack_path / "abnormal_traces.parquet")
    traces = pl.concat([normal_traces, anomal_traces])

    g = build_service_graph(traces)

    # Check if entry service exists in graph
    if entry not in g.nodes:
        logger.warning(f"Entry service '{entry}' not found in service graph")
        return []

    # Calculate ground truth paths and divergence paths for each prediction
    gt_paths = []
    for gt in groundtruth_items:
        if gt not in g.nodes:
            logger.warning(f"Ground truth service '{gt}' not found in service graph")
            continue

        try:
            # Get shortest path from entry to ground truth
            gt_path = _get_shortest_path(g, source=entry, target=gt)
            gt_paths.append((gt_path, gt))
        except nx.NetworkXNoPath:
            logger.warning(f"No path found from '{entry}' to '{gt}'")
            continue

    if len(gt_paths) == 0:
        logger.error(f"No valid ground truth paths found for entry '{entry}'")
        return []

    predictions_sorted = sorted(predictions, key=lambda x: getattr(x, "rank", 0))[:k]
    max_score = None
    alignment_scores = []
    for pre in predictions_sorted:
        algo_pre = pre.result
        assert algo_pre is not None

        if algo_pre not in g.nodes:
            logger.warning(f"Predicted service '{algo_pre}' not found in service graph")
            alignment_scores.append(0.0)
            continue

        algo_path = _get_shortest_path(g, source=entry, target=algo_pre)

        gt_divergence_path_pairs_for_algo = []
        for gt_path, gt in gt_paths:
            divergence_path = _get_shortest_path(g, source=algo_pre, target=gt)
            gt_divergence_path_pairs_for_algo.append((gt_path, divergence_path))

        # Calculate multi-path alignment score
        score = alignment_score_multi_gt(algo_path, gt_divergence_path_pairs_for_algo)
        if max_score is None or score > max_score:
            max_score = score
        alignment_scores.append(max_score)

    return alignment_scores


def calculate_efficiency_score(
    datapack_path: Path, entry: str, groundtruth_items: list[str], predictions: list[DtoGranularityRecord]
) -> float:
    try:
        normal_traces = pl.scan_parquet(datapack_path / "normal_traces.parquet")
        anomal_traces = pl.scan_parquet(datapack_path / "abnormal_traces.parquet")
        traces = pl.concat([normal_traces, anomal_traces])
        g = build_service_graph(traces)

        if entry not in g.nodes:
            logger.warning(f"Entry service '{entry}' not found in service graph")
            return 0.0

        pre_paths = [_get_shortest_path(g, source=entry, target=i.result) for i in predictions if i.result is not None]
        gt_paths = [_get_shortest_path(g, source=entry, target=i) for i in groundtruth_items if i in g.nodes]

        return effi([i for j in pre_paths for i in j], [i for j in gt_paths for i in j])
    except Exception:
        return 0.0


def get_metrics_by_dataset(
    algorithm: str,
    dataset: str,
    dataset_version: str | None = None,
    tag: str | None = None,
    base_url: str | None = None,
) -> list[AlgoMetrics]:
    evaluation = get_evaluation_by_dataset(algorithm, dataset, dataset_version, tag, base_url)

    assert evaluation is not None
    assert len(evaluation) > 0

    level_metrics: defaultdict[str, dict[str, float]] = defaultdict(
        lambda: {
            "top1": 0.0,
            "top3": 0.0,
            "top5": 0.0,
            "mrr": 0.0,
            "as1": 0.0,
            "as3": 0.0,
            "as5": 0.0,
            "efficiency": 0.0,
        }
    )
    total_datapacks = 0

    for resp in evaluation:
        assert resp.items is not None, "Response items are None"
        for item in resp.items:
            assert item.datapack_name is not None
            assert item.groundtruth is not None, f"Groundtruth is not found for datapack {item.datapack_name}"
            assert item.predictions is not None, f"Predictions are not found for datapack {item.datapack_name}"

            total_datapacks += 1

            # Extract ground truth items for each granularity level
            groundtruth_levels = {}
            if item.groundtruth.service:
                groundtruth_levels["service"] = item.groundtruth.service
            if item.groundtruth.span:
                groundtruth_levels["span"] = item.groundtruth.span
            if item.groundtruth.pod:
                groundtruth_levels["pod"] = item.groundtruth.pod
            if item.groundtruth.container:
                groundtruth_levels["container"] = item.groundtruth.container
            if item.groundtruth.function:
                groundtruth_levels["function"] = item.groundtruth.function
            if item.groundtruth.metric:
                groundtruth_levels["metric"] = item.groundtruth.metric

            # Calculate metrics for each level
            for level, groundtruth_items in groundtruth_levels.items():
                metrics = calculate_metrics_for_level(groundtruth_items, item.predictions, level)

                for metric_name, value in metrics.to_dict().items():
                    level_metrics[level][metric_name] += value

            # if level == "service":
            #     as_k = calculate_alignment_score(
            #         Path("data/rcabench_dataset") / item.datapack_name / "converted",
            #         "loadgenerator",
            #         groundtruth_items,
            #         item.predictions,
            #     )
            #     eff = calculate_efficiency_score(
            #         Path("data/rcabench_dataset") / item.datapack_name / "converted",
            #         "loadgenerator",
            #         groundtruth_items,
            #         item.predictions,
            #     )

            #     for idx, as_i in enumerate(as_k):
            #         level_metrics[level][f"as{idx + 1}"] = level_metrics[level].get(f"as{idx + 1}", 0.0) + as_i

            #     level_metrics[level]["efficiency"] = level_metrics[level].get("efficiency", 0.0) + eff

    # Average metrics across all datapacks
    result_metrics = []
    for level, metrics in level_metrics.items():
        if total_datapacks > 0:
            avg_metrics = {
                "top1": metrics["top1"] / total_datapacks,
                "top3": metrics["top3"] / total_datapacks,
                "top5": metrics["top5"] / total_datapacks,
                "mrr": metrics["mrr"] / total_datapacks,
                "as1": metrics["as1"] / total_datapacks,
                "as3": metrics["as3"] / total_datapacks,
                "as5": metrics["as5"] / total_datapacks,
                "efficiency": metrics["efficiency"] / total_datapacks,
            }
        else:
            avg_metrics = {
                "top1": 0.0,
                "top3": 0.0,
                "top5": 0.0,
                "mrr": 0.0,
                "as1": 0.0,
                "as3": 0.0,
                "as5": 0.0,
                "efficiency": 0.0,
            }

        result_metrics.append(
            AlgoMetrics(
                level=level,
                top1=round(avg_metrics["top1"], 3),
                top3=round(avg_metrics["top3"], 3),
                top5=round(avg_metrics["top5"], 3),
                mrr=round(avg_metrics["mrr"], 3),
                as1=round(avg_metrics["as1"], 3),
                as3=round(avg_metrics["as3"], 3),
                as5=round(avg_metrics["as5"], 3),
                efficiency=round(avg_metrics["efficiency"], 3),
                datapack_count=total_datapacks,
            )
        )

    return result_metrics


# =============================================================================
# Multi-Algorithm Comparison Functions
# =============================================================================


def get_algorithms_metrics_across_datasets(
    algorithms: list[str],
    datasets: list[str],
    dataset_versions: list[str] | None = None,
    tag: str | None = None,
    base_url: str | None = None,
    level: str | None = None,
) -> list[dict]:
    """Get metrics comparison for multiple algorithms across different datasets.

    Args:
        algorithms: List of algorithm names
        datasets: List of dataset names
        dataset_versions: List of dataset versions (optional, if None will use default versions)
        tag: Evaluation tag
        base_url: API base URL (optional)
        level: Granularity level, if None returns all levels

    Returns:
        List of dictionaries containing algorithm names, datasets, versions and corresponding metrics
    """
    result = []

    # If dataset_versions is not provided, use None for all datasets
    if dataset_versions is None:
        dsv = [None] * len(datasets)
    else:
        # Ensure datasets and dataset_versions have the same length
        if len(datasets) != len(dataset_versions):
            raise ValueError("The number of datasets and dataset versions must be the same")
        dsv = dataset_versions

    for algorithm in algorithms:
        for i, dataset in enumerate(datasets):
            dataset_version = dsv[i]
            try:
                metrics = get_metrics_by_dataset(algorithm, dataset, dataset_version, tag, base_url)

                if level is not None:
                    # Only return metrics for the specified level
                    level_metrics = [m for m in metrics if m["level"] == level]
                    if level_metrics:
                        result.append(
                            {
                                "algorithm": algorithm,
                                "dataset": dataset,
                                "dataset_version": dataset_version,
                                **level_metrics[0],
                            }
                        )
                else:
                    # Return metrics for all levels
                    for metric in metrics:
                        result.append(
                            {"algorithm": algorithm, "dataset": dataset, "dataset_version": dataset_version, **metric}
                        )
            except Exception as e:
                # If there's an error getting metrics for this combination, skip it
                raise e

    return result
