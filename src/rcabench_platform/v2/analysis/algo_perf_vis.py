from pathlib import Path
from typing import Literal, Union

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from ..logging import logger


def algo_perf_by_groups(
    df: pl.DataFrame,
    output_file: Path | None = None,
) -> None:
    if df.height == 0:
        logger.warning("No data available to generate chart")
        return

    # Check if we have the required columns for the new structure
    if "algorithm" not in df.columns:
        logger.warning("No 'algorithm' column found in data")
        return

    # Identify group columns (exclude algorithm and metric columns)
    metric_cols = ["top1", "top3", "top5", "mrr", "avg3", "avg5"]
    group_cols = [col for col in df.columns if col not in ["algorithm", "time", "count"] + metric_cols]
    if not group_cols:
        logger.warning("No group columns found in data")
        return

    group_title = " × ".join(group_cols)

    # Check for metric columns
    available_metrics = [col for col in ["top1", "top3", "top5", "mrr", "avg3", "avg5"] if col in df.columns]
    if not available_metrics:
        logger.warning("No performance metric data found")
        return

    algorithms = df["algorithm"].unique().to_list()
    algorithms = sorted([algo for algo in algorithms if algo is not None])

    if not algorithms:
        logger.warning("No valid algorithm data found")
        return

    # Get unique group combinations
    if len(group_cols) == 1:
        groups = df.select(group_cols[0]).unique().to_pandas()[group_cols[0]].tolist()
        groups = sorted([combo for combo in groups if combo is not None])
    else:
        group_df = df.select(group_cols).unique().to_pandas()
        groups = [tuple(row) for row in group_df.values if not any(val is None for val in row)]
        groups = sorted(groups)

    if not groups:
        logger.warning("No group data found")
        return

    # Set color mapping for metrics
    colors = cm.get_cmap("Set1")(np.linspace(0, 1, len(available_metrics)))
    metric_colors = {metric: colors[i] for i, metric in enumerate(available_metrics)}

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
        bar_width = 0.8 / len(available_metrics)  # Width of each bar

        for metric_idx, metric in enumerate(available_metrics):
            values = []

            for algorithm in algorithms:
                # Filter data for this algorithm
                algo_data = group_data.filter(pl.col("algorithm") == algorithm)
                if algo_data.height > 0 and metric in algo_data.columns:
                    value = algo_data[metric].to_list()[0]
                    values.append(value if value is not None else 0.0)
                else:
                    values.append(0.0)

            x_positions = x_pos + (metric_idx - len(available_metrics) / 2 + 0.5) * bar_width

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
            [metric.upper() for metric in available_metrics],
            loc="upper center",
            bbox_to_anchor=(0.5, 0.98),
            ncol=len(available_metrics),
            fontsize=12,
        )

    # Add common axis labels to the figure
    fig.text(0.5, 0.02, "Algorithm", ha="center", va="bottom", fontsize=12, fontweight="bold")
    fig.text(0.02, 0.5, "Performance Score", ha="center", va="center", rotation=90, fontsize=12, fontweight="bold")

    # Add title with grouping information
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
    k: int,
    output_file: Path | None = None,
) -> None:
    if df.height == 0:
        logger.warning("No data available to generate scatter chart")
        return

    # Check for required columns in new structure
    required_cols = ["fault_category", f"sdd_k{k}_category", "algorithm"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"Missing required columns {missing_cols} in data")
        return

    # Check for MRR and time metrics
    if "mrr" not in df.columns:
        logger.warning("No MRR data found")
        return

    if "time" not in df.columns:
        logger.warning("No time data found")
        return

    algorithms = df["algorithm"].unique().to_list()
    algorithms = sorted([algo for algo in algorithms if algo is not None])
    if not algorithms:
        logger.warning("No valid algorithm data found")
        return

    fault_categories = df["fault_category"].unique().to_list()
    fault_categories = sorted([cat for cat in fault_categories if cat is not None])

    sdd_groups = df[f"sdd_k{k}_category"].unique().to_list()
    sdd_groups = sorted([sdd for sdd in sdd_groups if sdd is not None])

    if not fault_categories or not sdd_groups:
        logger.warning("No fault category or SDD groups found")
        return

    # Find the maximum time value across all algorithms for y-axis scaling
    max_time_overall = 0
    time_values = df["time"].drop_nulls().to_list()
    if time_values:
        max_time_overall = max(time_values)

    # Set y-axis maximum to next order of magnitude for better comparison
    if max_time_overall > 0:
        # Calculate the order of magnitude
        import math

        order_of_magnitude = 10 ** math.ceil(math.log10(max_time_overall))
        y_max = order_of_magnitude
    else:
        y_max = 10

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

    for row_idx, sdd_group in enumerate(sdd_groups):
        for col_idx, fault_category in enumerate(fault_categories):
            ax = axes[row_idx, col_idx]

            if row_idx == n_rows - 1:
                ax.set_xlabel(f"{fault_category}", fontsize=9, fontweight="bold")

            if col_idx == 0:
                ax.set_ylabel(f"SDD: {sdd_group}", fontsize=9, fontweight="bold")

            group_data = df.filter(
                (pl.col("fault_category") == fault_category) & (pl.col(f"sdd_k{k}_category") == sdd_group)
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
                continue

            has_valid_data = False
            for algo in algorithms:
                # Filter data for this algorithm
                algo_data = group_data.filter(pl.col("algorithm") == algo)

                if algo_data.height > 0:
                    mrr_values = algo_data["mrr"].to_list()
                    time_values = algo_data["time"].to_list()

                    for mrr_value, time_value in zip(mrr_values, time_values):
                        if mrr_value is not None and time_value is not None:
                            has_valid_data = True
                            # Use real time value directly
                            real_time_value = max(time_value, 0.001)

                            ax.scatter(
                                mrr_value,
                                real_time_value,
                                color=algo_colors[algo],
                                label=algo if row_idx == 0 and col_idx == 0 else "",
                                s=50,
                                alpha=0.7,
                                edgecolors="black",
                                linewidth=0.5,
                            )

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
                continue

            ax.text(0.02, 0.98, "Time(s)", transform=ax.transAxes, fontsize=7, va="top", ha="left", alpha=0.7)
            ax.text(0.98, 0.02, "MRR", transform=ax.transAxes, fontsize=7, va="bottom", ha="right", alpha=0.7)

            ax.grid(True, alpha=0.3)

            ax.set_xlim(-0.05, 1.05)

            # Use linear scale for y-axis with equal intervals
            ax.set_yscale("linear")
            ax.set_ylim(0, y_max)

            if col_idx == 0:
                # Let matplotlib handle the linear scale ticks and labels automatically
                pass
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
