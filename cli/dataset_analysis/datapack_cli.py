#!/usr/bin/env -S uv run -s
import json
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
from rcabench_platform.v2.analysis.detector_visualization import batch_visualization
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.utils.dataframe import format_dataframe, print_dataframe
from rcabench_platform.v2.utils.serde import save_parquet

DEFAULT_NAMESPACE = "ts"
ALGORITHMS = [
    "baro",
    "simplerca",
    "microdig",
    "microhecl",
    "microrank",
    "microrca",
    "shapleyiq",
    "eadro",
    "diagfusion",
    "art",
    "nezha",
]
DEGREES = ["absolute_anomaly"]  # , "may_anomaly", "no_anomaly"]
METRICS = ["SDD@1", "SDD@3", "SDD@5", "CPL", "RootServiceDegree"]


load_dotenv()


@app.command(name="visualize")
def visualize(dataset_id: int, execution_tag: str | None = None) -> None:
    items, _ = get_execution_item(ALGORITHMS, dataset_id=dataset_id, execution_tag=execution_tag)

    count_items = build_items_with_cache(
        output_pkl_path=Path("temp/dataset_analysis/datapacks") / "injections" / "items.pkl",
        input_items=items,
        metrics=METRICS,
        namespace=DEFAULT_NAMESPACE,
    )

    df = aggregate(count_items)
    save_parquet(df, path="temp/algo/aggregated_result.parquet")


@app.command()
def analysis():
    df = pl.read_parquet("temp/algo/aggregated_result.parquet")

    aggregator = DuckDBAggregator(df)

    def vis_hook(df, name):
        format_dataframe(df, "html", output_file=f"temp/algo/{name}.html")
        algo_perf_by_groups(df, output_file=Path(f"temp/algo/{name}.png"))

    try:
        print_dataframe(aggregator.algorithm_performance_summary())
        vis_hook(aggregator.algorithm_performance_summary(), "performance_all")
        vis_hook(aggregator.fault_category(), "fault_category_analysis")
        vis_hook(aggregator.fault_type(), "fault_type_analysis")

        sdd1 = aggregator.sdd_k(1)
        sdd3 = aggregator.sdd_k(3)
        sdd5 = aggregator.sdd_k(5)

        print_dataframe(sdd1)
        print_dataframe(sdd3)
        print_dataframe(sdd5)

        # print_dataframe(aggregator.algorithm_performance_breakdown("baro"))
        algo = "microrank"
        format_dataframe(
            aggregator.algorithm_success_failure_stats(algo),
            output_format="html",
            output_file="temp/algo/algorithm_success_failure_stats.html",
        )
        format_dataframe(
            aggregator.algorithm_failure_characteristics(algo),
            output_format="html",
            output_file="temp/algo/algorithm_failure_characteristics.html",
        )
        format_dataframe(
            aggregator.algorithm_comparative_analysis(algo),
            output_format="html",
            output_file="temp/algo/algorithm_comparative_analysis.html",
        )
        format_dataframe(
            aggregator.algorithm_detailed_performance_matrix(algo),
            output_format="html",
            output_file="temp/algo/algorithm_detailed_performance_matrix.html",
        )

        fcasdd_df = aggregator.fault_category_and_sdd_analysis(1)
        format_dataframe(
            fcasdd_df,
            "html",
            output_file="temp/algo/fault_category_and_sdd_analysis.html",
        )

        # batch_visualization(datapack_paths, False)

        # aggregator.print_schema()

    finally:
        aggregator.close()


if __name__ == "__main__":
    app()
