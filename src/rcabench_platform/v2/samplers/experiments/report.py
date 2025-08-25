"""Sampler performance report generation."""

import polars as pl

from ...datasets.spec import get_datapack_list
from ...logging import logger, timeit
from ...utils.dataframe import print_dataframe
from ...utils.serde import save_parquet
from ..spec import SamplingMode, global_sampler_registry
from .spec import get_sampler_output_folder


@timeit(log_level="INFO")
def generate_sampler_perf_report(
    datasets: list[str],
    samplers: list[str] | None = None,
    sampling_rates: list[float] | None = None,
    modes: list[SamplingMode] | None = None,
    *,
    warn_missing: bool = False,
):
    """
    Generate performance report for sampler experiments.

    Args:
        datasets: List of dataset names to include in report
        samplers: List of sampler names (default: all registered)
        sampling_rates: List of sampling rates (default: scan for all used rates)
        modes: List of modes (default: both online and offline)
        warn_missing: Whether to warn about missing result files
    """
    if samplers is None:
        samplers = list(global_sampler_registry().keys())

    if modes is None:
        modes = [SamplingMode.ONLINE, SamplingMode.OFFLINE]

    all_perf_data = []

    for dataset in datasets:
        datapacks = get_datapack_list(dataset)
        logger.info(f"Processing dataset {dataset} with {len(datapacks)} datapacks")

        # If sampling_rates not specified, scan for available rates
        if sampling_rates is None:
            dataset_sampling_rates = _scan_available_sampling_rates(dataset, datapacks, samplers, modes)
        else:
            dataset_sampling_rates = sampling_rates

        logger.debug(f"Found sampling rates: {dataset_sampling_rates}")

        for sampler in samplers:
            for sampling_rate in dataset_sampling_rates:
                for mode in modes:
                    perf_files = []

                    for datapack in datapacks:
                        output_folder = get_sampler_output_folder(dataset, datapack, sampler, sampling_rate, mode)
                        perf_file = output_folder / "perf.parquet"

                        if perf_file.exists():
                            perf_files.append(perf_file)
                        elif warn_missing:
                            logger.warning(f"Missing perf file: {perf_file}")

                    if len(perf_files) == 0:
                        logger.warning(
                            f"No perf files found for {sampler} on {dataset} (rate={sampling_rate}, mode={mode.value})"
                        )
                        continue

                    logger.debug(f"Loading {len(perf_files)} perf files for {sampler}/{dataset}")

                    # Load and aggregate performance data
                    perf_df = pl.read_parquet(perf_files, rechunk=True)

                    # Add metadata
                    perf_df = perf_df.with_columns(
                        pl.lit(sampler).alias("sampler"),
                        pl.lit(dataset).alias("dataset"),
                        pl.lit(sampling_rate).alias("sampling_rate"),
                        pl.lit(mode.value).alias("mode"),
                    )

                    all_perf_data.append(perf_df)

    if len(all_perf_data) == 0:
        logger.warning("No performance data found")
        return

    # Combine all performance data
    combined_perf_df = pl.concat(all_perf_data, rechunk=True)

    # Calculate aggregate statistics
    agg_perf_df = (
        combined_perf_df.group_by(["sampler", "dataset", "sampling_rate", "mode"])
        .agg(
            [
                pl.len().alias("datapack_count"),
                pl.col("sampled_count").mean().alias("avg_sampled_count"),
                pl.col("total_traces").mean().alias("avg_total_traces"),
                pl.col("controllability").mean().alias("avg_controllability"),
                pl.col("comprehensiveness").mean().alias("avg_comprehensiveness"),
                pl.col("proportion_anomaly").mean().alias("avg_proportion_anomaly"),
                pl.col("proportion_rare").mean().alias("avg_proportion_rare"),
                pl.col("proportion_detector").mean().alias("avg_proportion_detector"),
                pl.col("actual_sampling_rate").mean().alias("avg_actual_sampling_rate"),
                pl.col("runtime_per_span_ns").mean().alias("avg_runtime_per_span_ns"),
                # Also calculate std dev for key metrics
                pl.col("controllability").std().alias("std_controllability"),
                pl.col("comprehensiveness").std().alias("std_comprehensiveness"),
                pl.col("actual_sampling_rate").std().alias("std_actual_sampling_rate"),
            ]
        )
        .sort(["sampler", "dataset", "sampling_rate", "mode"])
    )

    # Save detailed and aggregated results
    from ...config import get_config

    config = get_config()
    output_folder = config.output / "sampler_reports"
    output_folder.mkdir(parents=True, exist_ok=True)

    save_parquet(combined_perf_df, path=output_folder / "detailed_perf.parquet")
    save_parquet(agg_perf_df, path=output_folder / "aggregated_perf.parquet")

    # Print summary table
    display_df = agg_perf_df.select(
        [
            "sampler",
            "dataset",
            "sampling_rate",
            "mode",
            "datapack_count",
            "avg_controllability",
            "avg_comprehensiveness",
            "avg_proportion_anomaly",
            "avg_proportion_rare",
            "avg_proportion_detector",
            "avg_actual_sampling_rate",
            "avg_runtime_per_span_ns",
        ]
    )

    logger.info("Sampler Performance Summary:")
    print_dataframe(display_df)

    logger.info(f"Detailed results saved to: {output_folder}")


def _scan_available_sampling_rates(
    dataset: str, datapacks: list[str], samplers: list[str], modes: list[SamplingMode]
) -> list[float]:
    """Scan for available sampling rates in existing output folders."""
    from pathlib import Path

    from ...config import get_config

    config = get_config()
    rates = set()

    # Scan first few datapacks to find available rates
    scan_datapacks = datapacks[: min(5, len(datapacks))]

    for datapack in scan_datapacks:
        datapack_folder = config.output / "sampled" / dataset / datapack
        if not datapack_folder.exists():
            continue

        # Look for folders matching pattern: {sampler}_{rate}_{mode}
        for folder in datapack_folder.iterdir():
            if not folder.is_dir():
                continue

            folder_name = folder.name
            # Parse pattern: sampler_rate_mode
            parts = folder_name.split("_")
            if len(parts) >= 3:
                try:
                    # Find the rate part (should be a float)
                    for part in parts[1:-1]:  # Skip first (sampler) and last (mode)
                        rate = float(part)
                        if 0.0 <= rate <= 1.0:
                            rates.add(rate)
                            break
                except ValueError:
                    continue

    return sorted(list(rates)) if rates else [0.1, 0.2, 0.5]  # Default rates if none found
