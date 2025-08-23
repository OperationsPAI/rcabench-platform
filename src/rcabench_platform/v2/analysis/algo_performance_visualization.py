from pathlib import Path

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from ..logging import logger


def plot_algorithm_performance_by_fault_type(
    df: pl.DataFrame, output_dir: Path | None = None, save_format: str = "png"
) -> None:
    """
    Plot bar charts showing algorithm performance with metrics grouped by algorithm
    for all fault types in a single figure

    Args:
        df: DataFrame containing algorithm performance data with fault type and algorithm metric columns
        output_dir: Output directory, if None only display the chart
        figsize: Figure size for the combined chart
        save_format: Save format ('png', 'pdf', 'svg')
    """
    if df.height == 0:
        logger.warning("No data available to generate chart")
        return

    # Extract algorithm performance metric columns
    algo_metric_cols = [
        col for col in df.columns if col.startswith("avg_algo_") and col.endswith(("_top1", "_top3", "_top5", "_mrr"))
    ]

    if not algo_metric_cols:
        logger.warning("No algorithm performance metric data found")
        return

    # Extract algorithm names and metric types
    algorithms = set()
    metrics = ["top1", "top3", "top5", "mrr"]

    for col in algo_metric_cols:
        # Format: avg_algo_{algorithm}_{metric}
        parts = col.split("_")
        if len(parts) >= 4:
            algo_name = "_".join(parts[2:-1])  # Extract algorithm name
            algorithms.add(algo_name)

    algorithms = sorted(list(algorithms))

    if not algorithms:
        logger.warning("No valid algorithm data found")
        return

    # Check if fault_type column exists
    if "fault_category" not in df.columns:
        logger.warning("Missing fault_category column in data")
        return

    fault_types = df["fault_category"].unique().to_list()
    fault_types = sorted([ft for ft in fault_types if ft is not None])

    if not fault_types:
        logger.warning("No fault type data found")
        return

    # Set color mapping for metrics

    colors = cm.get_cmap("Set1")(np.linspace(0, 1, len(metrics)))
    metric_colors = {metric: colors[i] for i, metric in enumerate(metrics)}

    # Calculate subplot layout
    n_fault_types = len(fault_types)
    cols = min(6, n_fault_types)  # Maximum 6 columns per row
    rows = (n_fault_types + cols - 1) // cols  # Ceiling division

    # Adjust figure size based on actual layout
    adjusted_figsize = (cols * 4, rows * 4)  # 4 width per subplot, 4 height per subplot (reduced from 6)

    # Create figure with subplots
    fig, axes = plt.subplots(rows, cols, figsize=adjusted_figsize)

    # Ensure axes is always a flat array for easy indexing
    if rows == 1 and cols == 1:
        axes = [axes]
    elif rows == 1 or cols == 1:
        axes = axes.flatten() if hasattr(axes, "flatten") else [axes]
    else:
        axes = axes.flatten()

    # Plot each fault type in a subplot
    for idx, fault_type in enumerate(fault_types):
        ax = axes[idx]

        # Filter data for current fault type
        fault_data = df.filter(pl.col("fault_category") == fault_type)

        if fault_data.height == 0:
            logger.warning(f"No data found for fault type: {fault_type}")
            ax.text(0.5, 0.5, f"No data for\n{fault_type}", ha="center", va="center", transform=ax.transAxes)
            ax.set_xticks([])
            ax.set_yticks([])
            continue

        # Prepare data
        x_pos = np.arange(len(algorithms))
        bar_width = 0.8 / len(metrics)  # Width of each bar

        # Plot bars for each metric
        for metric_idx, metric in enumerate(metrics):
            values = []

            for algorithm in algorithms:
                col_name = f"avg_algo_{algorithm}_{metric}"
                if col_name in fault_data.columns:
                    value = fault_data[col_name].to_list()[0]
                    values.append(value if value is not None else 0.0)
                else:
                    values.append(0.0)

            # Calculate x positions for this metric's bars
            x_positions = x_pos + (metric_idx - len(metrics) / 2 + 0.5) * bar_width

            # Create bars
            bars = ax.bar(
                x_positions,
                values,
                bar_width,
                label=metric.upper() if idx == 0 else "",  # Only show legend on first subplot
                color=metric_colors[metric],
                alpha=0.8,
                edgecolor="black",
                linewidth=0.5,
            )

            # Add value labels on bars (only if values are different)
            unique_values = set(values)
            show_labels = len(unique_values) > 1 or (len(unique_values) == 1 and list(unique_values)[0] != 0)

            if show_labels:
                max_value = max(values) if values else 0
                for bar, value in zip(bars, values):
                    if value > 0:
                        height = bar.get_height()
                        # Stagger label heights to avoid overlap
                        label_offset = max_value * (0.02 + metric_idx * 0.025)
                        ax.text(
                            bar.get_x() + bar.get_width() / 2.0,
                            height + label_offset,
                            f"{value:.2f}",
                            ha="center",
                            va="bottom",
                            fontsize=7,  # Reduced font size
                            fontweight="bold",
                        )

        # Truncate long fault type names for title
        title_text = fault_type if len(fault_type) <= 20 else fault_type[:17] + "..."
        ax.set_title(title_text, fontsize=11, fontweight="bold")

        ax.set_xticks(x_pos)
        ax.set_xticklabels(algorithms, fontsize=9, rotation=45, ha="right")
        ax.grid(True, alpha=0.3, axis="y")

        # Set y-axis limits to accommodate labels
        ax.set_ylim(0, 1.15)  # Increased upper limit to accommodate staggered labels

    # Hide empty subplots
    total_subplots = rows * cols
    for idx in range(n_fault_types, total_subplots):
        axes[idx].set_visible(False)

    # Add legend to the figure
    if n_fault_types > 0:
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

    plt.tight_layout()
    plt.subplots_adjust(top=0.9, bottom=0.08, left=0.08)  # Make room for the legend, title and axis labels

    # Save or display chart
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"algorithm_performance_combined.{save_format}"
        filepath = output_dir / filename

        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        logger.warning(f"Combined chart saved to: {filepath}")

    plt.show()


def create_algorithm_performance_report(
    df: pl.DataFrame, output_dir: Path, title: str = "Algorithm Performance Analysis Report"
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_algorithm_performance_by_fault_type(df, output_dir)
