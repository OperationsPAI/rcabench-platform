from pathlib import Path
from typing import Literal, Union

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from ..logging import logger
from .aggregation import GroupSpec


def algo_perf_by_groups(
    df: pl.DataFrame,
    group_specs: list[GroupSpec] | None = None,
    output_file: Path | None = None,
    title_prefix: str = "",
) -> None:
    if df.height == 0:
        logger.warning("No data available to generate chart")
        return

    if group_specs is None:
        group_cols = [col for col in df.columns if col.startswith("group_")]
        if not group_cols:
            logger.warning("No group columns found in data")
            return
        group_title = " × ".join([col.replace("group_", "") for col in group_cols])
    else:
        group_cols = [f"group_{spec['column']}" for spec in group_specs]

        missing_cols = [col for col in group_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing group columns {missing_cols} in data")
            return

        group_descriptions = [spec["column"] for spec in group_specs]
        group_title = " × ".join(group_descriptions)

    algo_metric_cols = [
        col for col in df.columns if col.startswith("avg_algo_") and col.endswith(("_top1", "_top3", "_top5", "_mrr"))
    ]

    if not algo_metric_cols:
        logger.warning("No algorithm performance metric data found")
        return

    algorithms = set()
    metrics = ["top1", "top3", "top5", "mrr"]

    for col in algo_metric_cols:
        parts = col.split("_")
        if len(parts) >= 4:
            algo_name = "_".join(parts[2:-1])  # Extract algorithm name
            algorithms.add(algo_name)

    algorithms = sorted(list(algorithms))

    if not algorithms:
        logger.warning("No valid algorithm data found")
        return

    if len(group_cols) == 1:
        groups = df.select(group_cols[0]).unique().to_pandas()[group_cols[0]].tolist()
        groups = [combo for combo in groups if combo is not None]
        groups = _sort_groups_by_specs(groups, group_specs, single_column=True)
    else:
        group_df = df.select(group_cols).unique().to_pandas()
        groups = [tuple(row) for row in group_df.values if not any(val is None for val in row)]
        groups = _sort_groups_by_specs(groups, group_specs, single_column=False)

    if not groups:
        logger.warning("No group data found")
        return

    # Set color mapping for metrics
    colors = cm.get_cmap("Set1")(np.linspace(0, 1, len(metrics)))
    metric_colors = {metric: colors[i] for i, metric in enumerate(metrics)}

    # Calculate subplot layout
    n_groups = len(groups)
    cols = min(6, n_groups)  # Maximum 6 columns per row
    rows = (n_groups + cols - 1) // cols  # Ceiling division

    # Adjust figure size based on actual layout
    adjusted_figsize = (cols * 4, rows * 4)

    # Create figure with subplots
    fig, axes = plt.subplots(rows, cols, figsize=adjusted_figsize)

    # Ensure axes is always a flat array for easy indexing
    if rows == 1 and cols == 1:
        axes = [axes]
    elif rows == 1 or cols == 1:
        axes = axes.flatten() if hasattr(axes, "flatten") else [axes]
    else:
        axes = axes.flatten()

    # Plot each group in a subplot
    for idx, group in enumerate(groups):
        ax = axes[idx]

        # Filter data for current group
        if len(group_cols) == 1:
            group_data = df.filter(pl.col(group_cols[0]) == group)
            group_label = str(group)
        else:
            filter_conditions = []
            for i, col in enumerate(group_cols):
                filter_conditions.append(pl.col(col) == group[i])

            combined_filter = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_filter = combined_filter & condition

            group_data = df.filter(combined_filter)
            group_label = " × ".join(str(val) for val in group)

        if group_data.height == 0:
            logger.warning(f"No data found for group: {group}")
            ax.text(0.5, 0.5, f"No data for\n{group}", ha="center", va="center", transform=ax.transAxes)
            ax.set_xticks([])
            ax.set_yticks([])
            continue

        # Prepare data
        x_pos = np.arange(len(algorithms))
        bar_width = 0.8 / len(metrics)  # Width of each bar

        for metric_idx, metric in enumerate(metrics):
            values = []

            for algorithm in algorithms:
                col_name = f"avg_algo_{algorithm}_{metric}"
                if col_name in group_data.columns:
                    value = group_data[col_name].to_list()[0]
                    values.append(value if value is not None else 0.0)
                else:
                    values.append(0.0)

            x_positions = x_pos + (metric_idx - len(metrics) / 2 + 0.5) * bar_width

            ax.bar(
                x_positions,
                values,
                bar_width,
                label=metric.upper() if idx == 0 else "",  # Only show legend on first subplot
                color=metric_colors[metric],
                alpha=0.8,
                edgecolor="black",
                linewidth=0.5,
            )

        # Truncate long group names for title
        title_text = group_label if len(group_label) <= 20 else group_label[:17] + "..."
        ax.set_title(title_text, fontsize=11, fontweight="bold")

        ax.set_xticks(x_pos)
        ax.set_xticklabels(algorithms, fontsize=9, rotation=45, ha="right")
        ax.grid(True, alpha=0.3, axis="y")

        # Set y-axis limits to accommodate labels
        ax.set_ylim(0, 1.15)

    # Hide empty subplots
    total_subplots = rows * cols
    for idx in range(n_groups, total_subplots):
        axes[idx].set_visible(False)

    # Add legend to the figure
    if n_groups > 0:
        fig.legend(
            [metric.upper() for metric in metrics],
            loc="upper center",
            bbox_to_anchor=(0.5, 0.98),
            ncol=len(metrics),
            fontsize=12,
        )

    # Add common axis labels to the figure
    fig.text(0.5, 0.02, "Algorithm", ha="center", va="bottom", fontsize=12, fontweight="bold")
    fig.text(0.02, 0.5, "Performance Score", ha="center", va="center", rotation=90, fontsize=12, fontweight="bold")

    # Add title with grouping information
    if title_prefix:
        chart_title = f"{title_prefix}Algorithm Performance by {group_title}"
    else:
        chart_title = f"Algorithm Performance by {group_title}"
    fig.suptitle(chart_title, fontsize=14, fontweight="bold", y=0.99)

    plt.tight_layout()
    plt.subplots_adjust(top=0.9, bottom=0.08, left=0.08)  # Make room for the legend, title and axis labels

    # Save or display chart
    if output_file:
        parent = output_file.parent
        parent.mkdir(parents=True, exist_ok=True)

        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        logger.info(f"Chart saved to: {output_file}")

    plt.show()


def algo_perf_scatter_by_fault_category(
    df: pl.DataFrame,
    output_file: Path | None = None,
) -> None:
    def _get_time_bucket(time_value):
        if time_value is None:
            return None
        if time_value <= 1:
            return "≤1s"
        elif time_value <= 10:
            return "<=10s"
        elif time_value <= 100:
            return "<=100s"
        elif time_value <= 1000:
            return "<=1000s"
        else:
            return ">1000s"

    def _get_bucket_position(bucket):
        bucket_positions = {"≤1s": 0.5, "<=10s": 1.5, "<=100s": 2.5, "<=1000s": 3.5, ">1000s": 4.5}
        return bucket_positions.get(bucket, 0)

    if df.height == 0:
        logger.warning("No data available to generate scatter chart")
        return

    required_group_cols = ["group_fault_category", "group_SDD@1"]
    missing_cols = [col for col in required_group_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"Missing required group columns {missing_cols} in data")
        return

    algo_mrr_cols = [col for col in df.columns if col.startswith("avg_algo_") and col.endswith("_mrr")]

    time_related_cols = [col for col in df.columns if "duration" in col.lower() or "time" in col.lower()]

    if not algo_mrr_cols:
        logger.warning("No algorithm MRR data found")
        return

    if not time_related_cols:
        logger.warning("No time-related data found")
        return

    algorithms = set()
    for col in algo_mrr_cols:
        parts = col.split("_")
        if len(parts) >= 4:
            algo_name = "_".join(parts[2:-1])
            algorithms.add(algo_name)

    algorithms = sorted(list(algorithms))
    if not algorithms:
        logger.warning("No valid algorithm data found")
        return

    fault_categories = df.select("group_fault_category").unique().to_pandas()["group_fault_category"].tolist()
    fault_categories = [cat for cat in fault_categories if cat is not None]
    fault_categories = sorted(fault_categories)

    sdd_groups = df.select("group_SDD@1").unique().to_pandas()["group_SDD@1"].tolist()
    sdd_groups = [sdd for sdd in sdd_groups if sdd is not None]
    sdd_groups = sorted(sdd_groups)

    if not fault_categories or not sdd_groups:
        logger.warning("No fault category or SDD@1 groups found")
        return

    n_rows = len(sdd_groups)
    n_cols = len(fault_categories)

    fig_width = max(12, n_cols * 3)
    fig_height = max(6, n_rows * 2.5)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(fig_width, fig_height))

    if n_rows == 1 and n_cols == 1:
        axes = np.array([[axes]])
    elif n_rows == 1:
        axes = axes.reshape(1, -1)
    elif n_cols == 1:
        axes = axes.reshape(-1, 1)

    colors = cm.get_cmap("tab10")(np.linspace(0, 1, len(algorithms)))
    algo_colors = {algo: colors[i] for i, algo in enumerate(algorithms)}

    time_col = time_related_cols[0]

    for row_idx, sdd_group in enumerate(sdd_groups):
        for col_idx, fault_category in enumerate(fault_categories):
            ax = axes[row_idx, col_idx]

            if row_idx == n_rows - 1:
                ax.set_xlabel(f"{fault_category}", fontsize=9, fontweight="bold")

            if col_idx == 0:
                ax.set_ylabel(f"SDD: {sdd_group}", fontsize=9, fontweight="bold")

            group_data = df.filter(
                (pl.col("group_fault_category") == fault_category) & (pl.col("group_SDD@1") == sdd_group)
            )

            if group_data.height == 0:
                ax.text(
                    0.5,
                    0.5,
                    "NA",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                    fontsize=14,
                    fontweight="bold",
                    color="gray",
                )
                ax.set_xticks([])
                ax.set_yticks([])
                # continue

            max_time_value = 0
            has_valid_data = False
            for algo in algorithms:
                mrr_col = f"avg_algo_{algo}_mrr"

                if mrr_col in group_data.columns and time_col in group_data.columns:
                    mrr_values = group_data[mrr_col].to_list()
                    time_values = group_data[time_col].to_list()

                    for mrr_value, time_value in zip(mrr_values, time_values):
                        if mrr_value is not None and time_value is not None:
                            has_valid_data = True
                            time_bucket = _get_time_bucket(time_value)
                            bucket_position = _get_bucket_position(time_bucket)

                            ax.scatter(
                                mrr_value,
                                bucket_position,
                                color=algo_colors[algo],
                                label=algo if row_idx == 0 and col_idx == 0 else "",
                                s=50,
                                alpha=0.7,
                                edgecolors="black",
                                linewidth=0.5,
                            )
                            max_time_value = max(max_time_value, bucket_position)

            if not has_valid_data:
                ax.text(
                    0.5,
                    0.5,
                    "NA",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                    fontsize=14,
                    fontweight="bold",
                    color="gray",
                )
                ax.set_xticks([])
                ax.set_yticks([])
                # continue

            if max_time_value > 0:
                ax.text(0.02, 0.98, "Time(s)", transform=ax.transAxes, fontsize=7, va="top", ha="left", alpha=0.7)
                ax.text(0.98, 0.02, "MRR", transform=ax.transAxes, fontsize=7, va="bottom", ha="right", alpha=0.7)

            ax.grid(True, alpha=0.3)

            ax.set_xlim(-0.05, 1.05)
            ax.set_ylim(0, 5)
            ax.set_yticks([0.5, 1.5, 2.5, 3.5, 4.5])

            if col_idx == 0:
                ax.set_yticklabels(["≤1", "≤1e1", "≤1e2", "≤1e3", ">1e3"], fontsize=8)
            else:
                ax.set_yticklabels([])

            if row_idx == n_rows - 1:
                ax.tick_params(axis="x", labelsize=8)
            else:
                ax.set_xticklabels([])

    if algorithms:
        fig.legend(
            algorithms,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.0),
            ncol=min(len(algorithms), 6),
            fontsize=10,
        )

    plt.tight_layout()
    plt.subplots_adjust(top=0.85, bottom=0.08, left=0.08)

    if output_file:
        parent = output_file.parent
        parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        logger.info(f"Scatter chart saved to: {output_file}")

    plt.show()


def _sort_groups_by_specs(groups, group_specs, single_column=True):
    if not group_specs:
        if single_column:
            return sorted(groups, key=lambda x: str(x))
        else:
            return sorted(groups, key=lambda x: tuple(str(val) for val in x))

    def _get_sort_key_for_value(value, spec):
        if spec["type"] == "categorical":
            return str(value)
        elif spec["type"] == "numeric_bins":
            if isinstance(value, str) and value.startswith("["):
                try:
                    lower_bound = float(value.split(",")[0][1:])
                    return lower_bound
                except (ValueError, IndexError):
                    return str(value)
            else:
                return str(value)
        else:
            return str(value)

    if single_column:
        spec = group_specs[0] if group_specs else None
        if spec:
            return sorted(groups, key=lambda x: _get_sort_key_for_value(x, spec))
        else:
            return sorted(groups, key=lambda x: str(x))
    else:

        def _get_multi_sort_key(group_tuple):
            sort_keys = []
            for i, value in enumerate(group_tuple):
                if i < len(group_specs):
                    spec = group_specs[i]
                    sort_keys.append(_get_sort_key_for_value(value, spec))
                else:
                    sort_keys.append(str(value))
            return tuple(sort_keys)

        return sorted(groups, key=_get_multi_sort_key)
