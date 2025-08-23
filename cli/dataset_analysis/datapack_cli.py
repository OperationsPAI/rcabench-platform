#!/usr/bin/env -S uv run -s
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv

from rcabench_platform.v2.analysis.aggregation import aggregate, get_fault_type_stats
from rcabench_platform.v2.analysis.algo_performance_visualization import create_algorithm_performance_report
from rcabench_platform.v2.analysis.data_prepare import (
    build_items_with_cache,
    get_execution_item,
)
from rcabench_platform.v2.analysis.datapacks_analysis import (
    Distribution,
    get_datapacks_distribution,
)
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.utils.dataframe import format_dataframe

DEFAULT_NAMESPACE = "ts"
ALGORITHMS = ["baro", "simplerca", "microdig", "traceback"]
DEGREES = ["absolute_anomaly"]  # , "may_anomaly", "no_anomaly"]
METRICS = ["SDD@1", "CPL", "RootServiceDegree"]


load_dotenv()


@app.command(name="visualize")
def visualize(dataset_id: int | None = None, project_id: int | None = None) -> None:
    withDatasetID = dataset_id is not None
    withProjectID = project_id is not None

    if withDatasetID and withProjectID:
        logger.error("Please provide either dataset_id or project_id, not both.")
        return

    distributions: dict[str, Distribution] = {}
    items, _ = get_execution_item(ALGORITHMS, dataset_id, project_id, DEGREES)

    for degree, input_items in items.items():
        count_items = build_items_with_cache(
            output_pkl_path=Path("temp/dataset_analysis/datapacks") / "injections" / "items.pkl",
            input_items=input_items,
            metrics=METRICS,
            namespace=DEFAULT_NAMESPACE,
        )

        df = aggregate(count_items)

        format_dataframe(df, "html", output_file=f"temp/res_{degree}_raw.html")
        format_dataframe(df, "csv", output_file=f"temp/res_{degree}_raw.csv")

        agg_df = get_fault_type_stats(df)

        create_algorithm_performance_report(agg_df, Path("temp/algo"))

        format_dataframe(agg_df, "html", output_file=f"temp/res_{degree}.html")
        format_dataframe(agg_df, "csv", output_file=f"temp/res_{degree}.csv")

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
