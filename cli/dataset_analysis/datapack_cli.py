#!/usr/bin/env -S uv run -s
import json
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

from rcabench_platform.v2.analysis.aggregation import (
    DuckDBAggregator,
    aggregate,
)
from rcabench_platform.v2.analysis.algo_perf_vis import (
    algo_failure_by_fault_type_bar,
    algo_perf_by_fault_type,
    algo_perf_by_groups,
    dataset_anomaly_distribution,
)
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
    logger.info(f"get {len(items)} items for visualization")
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

    def vis_hook(df, name, fig=False):
        format_dataframe(df, "html", output_file=f"temp/algo/{name}.html")
        format_dataframe(df, "csv", output_file=f"temp/algo/{name}.csv")
        format_dataframe(df, "latex", output_file=f"temp/algo/{name}.tex")
        if fig:
            algo_perf_by_groups(df, output_file=Path(f"temp/algo/{name}.png"))

    try:
        vis_hook(aggregator.dataset_overall(), "dataset_overall")
        vis_hook(aggregator.dataset_fault_type(), "dataset_fault_type")

        dataset_anomaly_distribution(aggregator.dataset_fault_type(), Path("temp/algo/rq4_generation_process.pdf"))

        # vis_hook(aggregator.perf_overall(), "overall_perf")
        # vis_hook(aggregator.perf_group_by_fault_category(), "fault_category", True)

        vis_hook(aggregator.perf_common_failures(1, 3), "common_failures")
        # algo_perf_by_fault_type(aggregator.perf_group_by_fault_type(), Path("temp/algo/fault_type.pdf"))

        failure_data = aggregator.perf_algo_failure_by_fault_type()
        vis_hook(failure_data, "algo_failure_by_fault_type")

        # Create bar chart visualization for failure patterns
        algo_failure_by_fault_type_bar(failure_data, Path("temp/algo/rq5_failure_distribution_by_fault_type.pdf"))
        # sdd1 = aggregator.perf_sdd_k(1)
        # sdd3 = aggregator.perf_sdd_k(3)
        # sdd5 = aggregator.perf_sdd_k(5)

        # print_dataframe(sdd5)

        # aggregator.print_schema()

    finally:
        aggregator.close()


if __name__ == "__main__":
    app()
