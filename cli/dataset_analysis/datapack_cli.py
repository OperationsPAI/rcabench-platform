#!/usr/bin/env -S uv run -s
from pathlib import Path

from dotenv import load_dotenv

from rcabench_platform.v2.analysis.aggregation import (
    CategoricalGroupSpec,
    NumericBinsGroupSpec,
    aggregate,
    get_stats_by_group,
)
from rcabench_platform.v2.analysis.algo_perf_vis import algo_perf_by_groups, algo_perf_scatter_by_fault_category
from rcabench_platform.v2.analysis.data_prepare import (
    build_items_with_cache,
    get_execution_item,
)
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.utils.dataframe import format_dataframe

DEFAULT_NAMESPACE = "ts"
ALGORITHMS = ["baro", "simplerca", "microdig", "traceback", "microhecl", "microrank", "microrca", "shapleyiq", "ton"]
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

        ft = CategoricalGroupSpec(type="categorical", column="fault_type")
        fc = CategoricalGroupSpec(type="categorical", column="fault_category")
        sdd = NumericBinsGroupSpec(type="numeric_bins", column="SDD@1", bins=[0, 1, 10])

        df = aggregate(count_items, group_specs=[ft, fc, sdd])
        fc_df = get_stats_by_group(df, [fc, sdd])
        ft_df = get_stats_by_group(df, [ft])

        algo_perf_scatter_by_fault_category(fc_df, Path("temp/algo/fault_type_scatter.png"))
        algo_perf_by_groups(fc_df, [fc, sdd], Path(f"temp/algo/fault_category_{degree}.png"))
        algo_perf_by_groups(ft_df, [ft], Path(f"temp/algo/fault_type_{degree}.png"))

        format_dataframe(df, "html", output_file=f"temp/res_{degree}_raw.html")
        format_dataframe(fc_df, "html", output_file=f"temp/algo/fault_category_{degree}.html")
        format_dataframe(ft_df, "html", output_file=f"temp/algo/fault_type_{degree}.html")
        format_dataframe(df, "csv", output_file=f"temp/res_{degree}_raw.csv")
        format_dataframe(fc_df, "csv", output_file=f"temp/algo/fault_category_{degree}.csv")
        format_dataframe(ft_df, "csv", output_file=f"temp/algo/fault_type_{degree}.csv")


if __name__ == "__main__":
    app()
