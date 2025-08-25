#!/usr/bin/env -S uv run -s
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

from rcabench_platform.v2.analysis.aggregation import (
    DuckDBAggregator,
    aggregate,
)
from rcabench_platform.v2.analysis.algo_perf_vis import algo_perf_by_groups, algo_perf_scatter_by_fault_category
from rcabench_platform.v2.analysis.data_prepare import (
    build_items_with_cache,
    get_execution_item,
)
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.utils.dataframe import format_dataframe
from rcabench_platform.v2.utils.serde import save_parquet

DEFAULT_NAMESPACE = "ts"
ALGORITHMS = ["baro", "simplerca", "microdig", "microhecl", "microrank", "microrca", "shapleyiq", "ton"]
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

    items, _ = get_execution_item(ALGORITHMS, dataset_id, project_id, DEGREES)

    for degree, input_items in items.items():
        count_items = build_items_with_cache(
            output_pkl_path=Path("temp/dataset_analysis/datapacks") / "injections" / "items.pkl",
            input_items=input_items,
            metrics=METRICS,
            namespace=DEFAULT_NAMESPACE,
        )

        df = aggregate(count_items)
        save_parquet(df, path="temp/algo/aggregated_result.parquet")


@app.command()
def analysis():
    df = pl.read_parquet("temp/algo/aggregated_result.parquet")

    aggregator = DuckDBAggregator(df)

    try:
        fca_df = aggregator.fault_category_analysis()
        format_dataframe(fca_df, "html", output_file="temp/algo/fault_category_analysis.html")
        algo_perf_by_groups(fca_df, output_file=Path("temp/algo/fault_category_analysis.png"))

        fta_df = aggregator.fault_type_analysis()
        format_dataframe(fta_df, "html", output_file="temp/algo/fault_type_analysis.html")
        algo_perf_by_groups(fta_df, output_file=Path("temp/algo/fault_type_analysis.png"))

        sdd_df = aggregator.sdd_analysis()
        format_dataframe(sdd_df, "html", output_file="temp/algo/sdd_analysis.html")
        algo_perf_by_groups(sdd_df, output_file=Path("temp/algo/sdd_analysis.png"))

        fcasdd_df = aggregator.fault_category_and_sdd_analysis()
        format_dataframe(
            fcasdd_df,
            "html",
            output_file="temp/algo/fault_category_and_sdd_analysis.html",
        )
        algo_perf_by_groups(fcasdd_df, output_file=Path("temp/algo/fault_category_and_sdd_analysis.png"))
        algo_perf_scatter_by_fault_category(
            fcasdd_df, output_file=Path("temp/algo/fault_category_and_sdd_analysis_scatter.png")
        )

        # aggregator.print_schema()
    finally:
        aggregator.close()


if __name__ == "__main__":
    app()
