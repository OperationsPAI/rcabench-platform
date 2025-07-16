#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger
from rcabench_platform.v2.datasets.spec import get_dataset_meta_file

import polars as pl
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path


def __create_visualizations(df: pl.DataFrame, rule_columns: list[str]):
    plt.style.use("default")

    import matplotlib.cm as cm

    colors = cm.get_cmap("tab10")(np.linspace(0, 1, len(rule_columns)))

    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle("Rules Check Distribution Analysis", fontsize=16, fontweight="bold")

    ax1 = axes[0, 0]
    rule_counts = []
    rule_labels = []

    for rule in rule_columns:
        count = df.filter(pl.col(rule)).height
        rule_counts.append(count)
        rule_labels.append(rule.replace("rule_", "R").replace("_", " ").title())

    bars = ax1.bar(range(len(rule_counts)), rule_counts, color=colors)
    ax1.set_xlabel("Rules")
    ax1.set_ylabel("Number of Filtered Datasets")
    ax1.set_title("Datasets Filtered by Each Rule")
    ax1.set_xticks(range(len(rule_labels)))
    ax1.set_xticklabels(rule_labels, rotation=45, ha="right")

    # Add numerical labels on bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width() / 2.0,
            height + max(rule_counts) * 0.01,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )

    ax2 = axes[0, 1]
    rules_per_dataset = df.select([pl.col(rule).cast(pl.Int32) for rule in rule_columns]).sum_horizontal()
    rules_counts = rules_per_dataset.to_numpy()

    ax2.hist(rules_counts, bins=range(8), alpha=0.7, color="skyblue", edgecolor="black")
    ax2.set_xlabel("Number of Rules Triggered per Dataset")
    ax2.set_ylabel("Number of Datasets")
    ax2.set_title("Distribution of Rules per Dataset")
    ax2.set_xticks(range(7))

    ax3 = axes[1, 0]
    rule_matrix = df.select([pl.col(rule).cast(pl.Int32) for rule in rule_columns]).to_numpy()
    correlation_matrix = np.corrcoef(rule_matrix.T)

    im = ax3.imshow(correlation_matrix, cmap="coolwarm", aspect="auto", vmin=-1, vmax=1)
    ax3.set_title("Rule Correlation Matrix")
    ax3.set_xticks(range(len(rule_columns)))
    ax3.set_yticks(range(len(rule_columns)))
    ax3.set_xticklabels([f"R{i + 1}" for i in range(len(rule_columns))], rotation=45)
    ax3.set_yticklabels([f"R{i + 1}" for i in range(len(rule_columns))])

    for i in range(len(rule_columns)):
        for j in range(len(rule_columns)):
            ax3.text(
                j,
                i,
                f"{correlation_matrix[i, j]:.2f}",
                ha="center",
                va="center",
                color="black" if abs(correlation_matrix[i, j]) < 0.5 else "white",
            )

    cbar = plt.colorbar(im, ax=ax3, shrink=0.8)
    cbar.set_label("Correlation Coefficient")

    ax4 = axes[1, 1]

    total_datasets = len(df)
    cumulative_filtered = []
    remaining = total_datasets

    for i, rule in enumerate(rule_columns):
        if i == 0:
            filtered = df.filter(pl.col(rule)).height
        else:
            prev_mask = pl.any_horizontal([pl.col(r) for r in rule_columns[:i]])
            current_mask = pl.any_horizontal([pl.col(r) for r in rule_columns[: i + 1]])
            filtered = df.filter(current_mask & ~prev_mask).height

        cumulative_filtered.append(filtered)
        remaining -= filtered

    cumulative_filtered.append(remaining)

    labels = [f"R{i + 1}" for i in range(len(rule_columns))] + ["Remaining"]
    colors = cm.get_cmap("tab10")(np.linspace(0, 1, len(labels)))

    ax4.bar(["Datasets"], [total_datasets], color="lightgray", alpha=0.3, label="Total")

    bottom = 0
    for i, (count, label, color) in enumerate(zip(cumulative_filtered, labels, colors)):
        ax4.bar(["Datasets"], [count], bottom=bottom, color=color, label=label, alpha=0.8)
        if count > 0:
            ax4.text(0, bottom + count / 2, f"{count}", ha="center", va="center", fontweight="bold")
        bottom += count

    ax4.set_ylabel("Number of Datasets")
    ax4.set_title("Cumulative Filtering Effect")
    ax4.legend(bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.tight_layout()

    output_path = Path("temp/rules_analysis.png")
    output_path.parent.mkdir(exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    logger.info(f"Visualization chart saved to: {output_path}")

    plt.show()


def __generate_statistics_report(df: pl.DataFrame, rule_columns: list[str]):
    print("\n" + "=" * 60)
    print("RULES CHECK ANALYSIS REPORT")
    print("=" * 60)

    total_datasets = len(df)
    print(f"Total number of datasets: {total_datasets}")

    print("\n1. Individual rule filtering statistics:")
    print("-" * 40)

    for i, rule in enumerate(rule_columns, 1):
        filtered_count = df.filter(pl.col(rule)).height
        percentage = (filtered_count / total_datasets) * 100
        rule_name = rule.replace("_", " ").title()
        print(f"Rule {i} ({rule_name}): {filtered_count} datasets ({percentage:.1f}%)")

    print("\n2. Comprehensive filtering statistics:")
    print("-" * 40)

    # Calculate datasets that trigger any rule
    any_rule_mask = pl.any_horizontal([pl.col(rule) for rule in rule_columns])
    failed_any = df.filter(any_rule_mask).height
    passed_all = total_datasets - failed_any

    print(f"Filtered out by any rule: {failed_any} datasets ({(failed_any / total_datasets) * 100:.1f}%)")
    print(f"Passed all rules: {passed_all} datasets ({(passed_all / total_datasets) * 100:.1f}%)")

    print("\n3. Number of rules triggered per dataset statistics:")
    print("-" * 40)

    rules_per_dataset = df.select([pl.col(rule).cast(pl.Int32) for rule in rule_columns]).sum_horizontal()
    rules_counts = rules_per_dataset.to_numpy()

    for i in range(7):
        count = np.sum(rules_counts == i)
        percentage = (count / total_datasets) * 100
        print(f"Triggered {i} rules: {count} datasets ({percentage:.1f}%)")

    avg_rules = np.mean(rules_counts)
    print(f"\nAverage number of rules triggered per dataset: {avg_rules:.2f}")

    print("\n4. Rule combination analysis:")
    print("-" * 40)

    # Find the most common rule combinations
    rule_combinations = df.select(rule_columns).to_pandas()
    combination_counts = rule_combinations.value_counts().head(10)

    print("Most common rule combinations (Top 10):")
    for i, (combination, count) in enumerate(combination_counts.items(), 1):
        if isinstance(combination, tuple):
            triggered_rules = [f"R{j + 1}" for j, val in enumerate(combination) if val]
        else:
            # Handle single boolean value case
            triggered_rules = ["R1"] if combination else []

        if triggered_rules:
            rules_str = " + ".join(triggered_rules)
        else:
            rules_str = "No rules triggered"
        print(f"  {i}. {rules_str}: {count} datasets")

    print("\n" + "=" * 60)


@app.command()
def run() -> None:
    rules_check_file = get_dataset_meta_file("rcabench", "rules_check.parquet")

    if not Path(rules_check_file).exists():
        print(f"Error: File does not exist {rules_check_file}")
        return

    df = pl.read_parquet(rules_check_file)
    print(f"Read rule check results for {len(df)} data packages")

    rule_columns = [
        "rule_1_network_no_direct_calls",
        "rule_2_http_method_same",
        "rule_3_http_no_direct_calls",
        "rule_4_single_point_no_calls",
        "rule_5_duplicated_spans",
        "rule_6_large_latency_normal",
    ]

    __create_visualizations(df, rule_columns)


if __name__ == "__main__":
    app()
