import os
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TypedDict

import networkx as nx
from rcabench.openapi import (
    DtoAlgorithmDatapackEvaluationResp,
    DtoAlgorithmDatasetEvaluationResp,
    DtoGranularityRecord,
    EvaluationApi,
)

from ..clients.rcabench_ import RCABenchClient
from ..logging import logger
from ..sources.rcabench import _build_service_graph


def _get_shortest_path_as_list(graph: nx.Graph, source: str, target: str) -> list[str]:
    assert source in graph and target in graph, "Source or target not in graph"
    path_result = nx.shortest_path(graph, source=source, target=target)
    return list(path_result) if isinstance(path_result, (list, tuple)) else []


class AlgoMetrics(TypedDict):
    level: str
    top1: float
    top3: float
    top5: float
    mrr: float
    datapack_count: int


def get_evaluation_by_datapack(
    algorithm: str, datapack: str, tag: str | None = None, base_url: str | None = None
) -> DtoAlgorithmDatapackEvaluationResp:
    base_url = base_url or os.getenv("RCABENCH_BASE_URL")
    assert base_url is not None, "base_url or RCABENCH_BASE_URL is not set"
    assert tag, "Tag must be specified."

    with RCABenchClient(base_url=base_url) as client:
        api = EvaluationApi(client)
        resp = api.api_v2_evaluations_algorithms_algorithm_datapacks_datapack_get(
            algorithm=algorithm,
            datapack=datapack,
            tag=tag,
        )

    assert resp.code is not None and resp.code < 300, f"Failed to get evaluation: {resp.message}"
    assert resp.data is not None
    return resp.data


def get_evaluation_by_dataset(
    algorithm: str,
    dataset: str,
    dataset_version: str | None = None,
    tag: str | None = None,
    base_url: str | None = None,
) -> DtoAlgorithmDatasetEvaluationResp:
    base_url = base_url or os.getenv("RCABENCH_BASE_URL")
    assert base_url is not None, "base_url or RCABENCH_BASE_URL is not set"
    assert tag, "Tag must be specified."

    with RCABenchClient(base_url=base_url) as client:
        api = EvaluationApi(client)
        resp = api.api_v2_evaluations_algorithms_algorithm_datasets_dataset_get(
            algorithm=algorithm,
            dataset=dataset,
            dataset_version=dataset_version,
            tag=tag,
        )

    assert resp.code is not None and resp.code < 300, f"Failed to get evaluation: {resp.message}"
    assert resp.data is not None
    return resp.data


def _as_vertices(path: Iterable[str]) -> set[str]:
    return set(path)


def _cpl(path: Sequence[str]) -> int:
    n = len(path)
    return n - 1 if n > 0 else 0


def wcpl(
    path: Iterable[str],
    weights: dict[tuple[str, str], float] | None = None,
) -> float:
    """wCPL(P): Weighted path length.

        Calculate the sum of the weights of all edges on the path:
    - If weights (edge weight dictionary, key is (src, dst) tuple) is provided,
        return the sum of the weights of each edge.
    - If not provided, it falls back to CPL(P): number of edges = max(len(path_list)-1, 0).
    """

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
    """Jaccard(P_algo, P_gt) = |V(P_gt) ∩ V(P_algo)| / |V(P_gt) ∪ V(P_algo)|。"""
    va = _as_vertices(algo_path)
    vg = _as_vertices(gt_path)
    union = va | vg
    if not union:
        return 1.0
    inter = va & vg
    return float(len(inter) / len(union))


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
    """AS_sp(P_algo, P_gt) = WDD(P_algo, P_gt) x Jaccard(P_algo, P_gt)"""
    return weighted_divergence_distance(algo_path, gt_path, divergence_path, weights) * jaccard_score(
        algo_path, gt_path
    )


def multi_path_alignment_score(
    algo_path: list[str],
    gt_divergence_path_pairs: list[tuple[list[str], list[str]]],
    weights: dict[tuple[str, str], float] | None = None,
) -> float:
    """AS_mp = max_i AS_sp(P_algo, P_gt^{(i)})。"""
    best = 0.0
    for gt, div in gt_divergence_path_pairs:
        score = single_path_alignment_score(algo_path, gt, div, weights=weights)
        if score > best:
            best = score
    return best


def calculate_metrics_for_level(
    groundtruth_items: list[str], predictions: list[DtoGranularityRecord], level: str
) -> dict[str, float]:
    """
    Calculates metrics at a specific granularity level

    Args:
    groundtruth_items: List of groundtruth labels for the granularity level
    predictions: List of algorithm predictions
    level: Name of the granularity level

    Returns:
    Dictionary containing top1, top3, top5, and mrr
    """
    if not groundtruth_items or not predictions:
        return {"top1": 0.0, "top3": 0.0, "top5": 0.0, "mrr": 0.0}

    level_predictions = [p for p in predictions if p.level == level]

    if not level_predictions:
        return {"top1": 0.0, "top3": 0.0, "top5": 0.0, "mrr": 0.0}

    level_predictions.sort(key=lambda x: x.rank or float("inf"))

    hits = []
    for pred in level_predictions:
        if pred.result in groundtruth_items:
            hits.append(pred.rank or float("inf"))

    if not hits:
        return {"top1": 0.0, "top3": 0.0, "top5": 0.0, "mrr": 0.0}

    min_rank = min(hits)
    top1 = 1.0 if min_rank <= 1 else 0.0
    top3 = 1.0 if min_rank <= 3 else 0.0
    top5 = 1.0 if min_rank <= 5 else 0.0

    mrr = 1.0 / min_rank

    return {"top1": top1, "top3": top3, "top5": top5, "mrr": mrr}


def calculate_alignment_score(
    datapack_name: str, entry: str, groundtruth_items: list[str], predictions: list[DtoGranularityRecord]
) -> dict[str, float]:
    g = _build_service_graph(Path(datapack_name))

    # Check if entry service exists in graph
    if entry not in g.nodes:
        logger.warning(f"Entry service '{entry}' not found in service graph")
        return {}

    # Calculate ground truth paths and divergence paths for each prediction
    gt_paths = []
    for gt in groundtruth_items:
        if gt not in g.nodes:
            logger.warning(f"Ground truth service '{gt}' not found in service graph")
            continue

        try:
            # Get shortest path from entry to ground truth
            gt_path = _get_shortest_path_as_list(g, source=entry, target=gt)
            gt_paths.append((gt_path, gt))
        except nx.NetworkXNoPath:
            logger.warning(f"No path found from '{entry}' to '{gt}'")
            continue

    if len(gt_paths) == 0:
        logger.error(f"No valid ground truth paths found for entry '{entry}'")
        return {}

    alignment_scores = {}
    for pre in predictions:
        algo_pre = pre.result
        assert algo_pre is not None

        if algo_pre not in g.nodes:
            logger.warning(f"Predicted service '{algo_pre}' not found in service graph")
            alignment_scores[algo_pre] = 0.0
            continue

        algo_path = _get_shortest_path_as_list(g, source=entry, target=algo_pre)

        gt_divergence_path_pairs_for_algo = []
        for gt_path, gt in gt_paths:
            divergence_path = _get_shortest_path_as_list(g, source=algo_pre, target=gt)
            gt_divergence_path_pairs_for_algo.append((gt_path, divergence_path))

        # Calculate multi-path alignment score
        score = multi_path_alignment_score(algo_path, gt_divergence_path_pairs_for_algo)
        alignment_scores[algo_pre] = score

    return alignment_scores


def get_metrics_by_dataset(
    algorithm: str,
    dataset: str,
    dataset_version: str | None = None,
    tag: str | None = None,
    base_url: str | None = None,
) -> list[AlgoMetrics]:
    evaluation = get_evaluation_by_dataset(algorithm, dataset, dataset_version, tag, base_url)

    assert evaluation.items is not None
    assert len(evaluation.items) > 0

    level_metrics: defaultdict[str, dict[str, float]] = defaultdict(
        lambda: {"top1": 0.0, "top3": 0.0, "top5": 0.0, "mrr": 0.0}
    )
    total_datapacks = 0

    for item in evaluation.items:
        assert item.datapack_name is not None
        assert item.groundtruth is not None, f"Groundtruth is not found for datapack {item.datapack_name}"
        assert item.predictions is not None, f"Predictions are not found for datapack {item.datapack_name}"

        total_datapacks += 1

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

        for level, groundtruth_items in groundtruth_levels.items():
            metrics = calculate_metrics_for_level(groundtruth_items, item.predictions, level)

            for metric_name, value in metrics.items():
                level_metrics[level][metric_name] += value

            if level == "service":
                scores = calculate_alignment_score(
                    item.datapack_name, "loadgenerator", groundtruth_items, item.predictions
                )
                print(scores)

    result_metrics = []
    for level, metrics in level_metrics.items():
        if total_datapacks > 0:
            avg_metrics = {
                "top1": metrics["top1"] / total_datapacks,
                "top3": metrics["top3"] / total_datapacks,
                "top5": metrics["top5"] / total_datapacks,
                "mrr": metrics["mrr"] / total_datapacks,
            }
        else:
            avg_metrics = {"top1": 0.0, "top3": 0.0, "top5": 0.0, "mrr": 0.0}

        result_metrics.append(
            AlgoMetrics(
                level=level,
                top1=round(avg_metrics["top1"], 3),
                top3=round(avg_metrics["top3"], 3),
                top5=round(avg_metrics["top5"], 3),
                mrr=round(avg_metrics["mrr"], 3),
                datapack_count=total_datapacks,
            )
        )

    return result_metrics


def get_multi_algorithms_metrics_by_dataset(
    algorithms: list[str],
    dataset: str,
    dataset_version: str | None = None,
    tag: str | None = None,
    base_url: str | None = None,
    level: str | None = None,
) -> list[dict]:
    """
    Get metrics comparison for multiple algorithms on the same (dataset, version)

    Args:
        algorithms: List of algorithm names
        dataset: Dataset name
        dataset_version: Dataset version
        tag: Tag
        base_url: Base URL
        level: Granularity level, if None returns all levels

    Returns:
        List of dictionaries containing algorithm names and corresponding metrics
    """
    result = []

    for algorithm in algorithms:
        metrics = get_metrics_by_dataset(algorithm, dataset, dataset_version, tag, base_url)

        if level is not None:
            # Only return metrics for the specified level
            level_metrics = [m for m in metrics if m["level"] == level]
            if level_metrics:
                result.append({"algorithm": algorithm, **level_metrics[0]})
        else:
            # Return metrics for all levels
            for metric in metrics:
                result.append({"algorithm": algorithm, **metric})

    return result


def get_algorithms_metrics_across_datasets(
    algorithms: list[str],
    datasets: list[str],
    dataset_versions: list[str] | None = None,
    tag: str | None = None,
    base_url: str | None = None,
    level: str | None = None,
) -> list[dict]:
    """
    Get metrics comparison for multiple algorithms across different datasets and versions

    Args:
        algorithms: List of algorithm names
        datasets: List of dataset names
        dataset_versions: List of dataset versions (optional, if None will use default versions)
        tag: Tag
        base_url: Base URL
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
                logger.warning(
                    f"Warning: Failed to get metrics for algorithm={algorithm}, dataset={dataset}, version={dataset_version}: {e}"  # noqa: E501
                )
                continue

    return result
