import os
from collections import defaultdict
from typing import TypedDict

from rcabench.openapi import (
    DtoAlgorithmDatapackEvaluationResp,
    DtoAlgorithmDatasetEvaluationResp,
    DtoGranularityRecord,
    EvaluationApi,
)

from ..clients.rcabench_ import RCABenchClient
from ..logging import logger


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


def calculate_metrics_for_level(
    groundtruth_items: list[str], predictions: list[DtoGranularityRecord], level: str
) -> dict[str, float]:
    """
    计算特定粒度级别的指标

    Args:
        groundtruth_items: 该粒度级别的真实标签列表
        predictions: 算法预测结果列表
        level: 粒度级别名称

    Returns:
        包含top1, top3, top5, mrr的字典
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
