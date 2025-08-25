"""Single sampler execution module."""

import dataclasses
import time
import traceback
from pathlib import Path

import polars as pl

from ...datasets.spec import get_datapack_folder
from ...datasets.train_ticket import extract_path
from ...logging import logger, timeit
from ...utils.fs import running_mark
from ...utils.serde import save_parquet
from ..spec import SamplerArgs, SamplingMode, global_sampler_registry
from .spec import get_sampler_output_folder


@timeit(log_level="INFO")
def run_sampler_single(
    sampler: str,
    dataset: str,
    datapack: str,
    sampling_rate: float,
    mode: SamplingMode,
    *,
    clear: bool = False,
    skip_finished: bool = True,
):
    """
    Run a single sampler on a datapack.

    Args:
        sampler: Name of the sampler algorithm
        dataset: Dataset name
        datapack: Datapack name
        sampling_rate: Sampling rate (0.0 to 1.0)
        mode: Sampling mode (online/offline)
        clear: Whether to clear existing output
        skip_finished: Whether to skip if already finished
    """
    sampler_instance = global_sampler_registry()[sampler]()

    input_folder = get_datapack_folder(dataset, datapack)
    output_folder = get_sampler_output_folder(dataset, datapack, sampler, sampling_rate, mode)

    with running_mark(output_folder, clear=clear):
        finished = output_folder / ".finished"
        if skip_finished and finished.exists():
            logger.debug(f"skipping {output_folder}")
            return

        try:
            t0 = time.time()
            sample_results = sampler_instance(
                SamplerArgs(
                    dataset=dataset,
                    datapack=datapack,
                    input_folder=input_folder,
                    output_folder=output_folder,
                    sampling_rate=sampling_rate,
                    mode=mode,
                )
            )
            t1 = time.time()
            runtime = t1 - t0
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error in {sampler} for {dataset}/{datapack}: {repr(e)}")
            sample_results = []
            runtime = None

    # Convert results to dataframe
    if len(sample_results) == 0:
        # Create empty result
        results_df = pl.DataFrame(schema={"trace_id": pl.String, "sample_score": pl.Float64})
    else:
        results_data = [dataclasses.asdict(result) for result in sample_results]
        results_df = pl.DataFrame(results_data)

    # Calculate performance metrics
    perf_metrics = calculate_sampler_performance(input_folder, results_df, sampling_rate, mode, runtime)

    # Add metadata to results
    output_df = results_df.with_columns(
        pl.lit(sampler).alias("sampler"),
        pl.lit(dataset).alias("dataset"),
        pl.lit(datapack).alias("datapack"),
        pl.lit(sampling_rate).alias("sampling_rate"),
        pl.lit(mode.value).alias("mode"),
        pl.lit(runtime, dtype=pl.Float64).alias("runtime.seconds"),
    )

    # Save results
    mode_filename = f"{mode.value}.parquet"
    save_parquet(output_df, path=output_folder / mode_filename)

    # Save performance metrics
    perf_df = pl.DataFrame([perf_metrics])
    save_parquet(perf_df, path=output_folder / "perf.parquet")

    finished.touch()

    logger.info(f"Sampler {sampler} completed for {dataset}/{datapack}")
    logger.info(f"Sampled {len(results_df)} traces in {runtime:.3f}s")
    logger.info(f"Performance metrics: {perf_metrics}")


def calculate_sampler_performance(
    input_folder: Path,
    sampled_df: pl.DataFrame,
    sampling_rate: float,
    mode: SamplingMode,
    runtime: float | None,
) -> dict:
    """
    Calculate performance metrics for sampler.

    Returns:
        Dictionary containing performance metrics:
        - controllability (RoD): Rate of Deviation
        - comprehensiveness (CR): Coverage Rate
        - proportion_anomaly (PRO_anomaly): Proportion of anomaly traces
        - proportion_rare (PRO_rare): Proportion of rare traces
        - proportion_detector (PRO_detector): Proportion of detector flagged traces
        - actual_sampling_rate: Actual sampling rate achieved
        - runtime_per_span_ns: Runtime per span in nanoseconds
    """
    # Load traces to get total counts
    normal_traces_lf = pl.scan_parquet(input_folder / "normal_traces.parquet")
    abnormal_traces_lf = pl.scan_parquet(input_folder / "abnormal_traces.parquet")

    # Get total unique traces
    all_traces_lf = pl.concat([normal_traces_lf.select("trace_id"), abnormal_traces_lf.select("trace_id")]).unique()

    total_traces = all_traces_lf.select(pl.len()).collect().item()
    sampled_count = len(sampled_df)

    # Calculate actual sampling rate
    actual_sampling_rate = sampled_count / total_traces if total_traces > 0 else 0.0

    # Calculate controllability (RoD)
    expected_count = int(total_traces * sampling_rate)
    controllability = abs((sampled_count - expected_count) / expected_count) if expected_count > 0 else 0.0

    if sampled_count == 0:
        return {
            "sampled_count": sampled_count,
            "total_traces": total_traces,
            "controllability": controllability,
            "comprehensiveness": 0.0,
            "proportion_anomaly": 0.0,
            "proportion_rare": 0.0,
            "proportion_detector": 0.0,
            "actual_sampling_rate": actual_sampling_rate,
            "runtime_per_span_ns": runtime * 1e9 / total_traces if runtime and total_traces > 0 else None,
        }

    # Load full traces with parsed span names for analysis
    combined_traces_lf = pl.concat(
        [
            normal_traces_lf.with_columns(pl.lit(False).alias("is_abnormal")),
            abnormal_traces_lf.with_columns(pl.lit(True).alias("is_abnormal")),
        ]
    )

    # Add parsed span names (entry points)
    traces_with_entry_lf = combined_traces_lf.with_columns(
        pl.col("span_name").map_elements(extract_path, return_dtype=pl.String).alias("entry_span")
    )

    # Get unique entry spans per trace (focusing on entry points)
    trace_entries_lf = (
        traces_with_entry_lf.filter(pl.col("parent_span_id").is_null())  # Entry spans typically have no parent
        .group_by("trace_id")
        .agg([pl.first("entry_span").alias("entry_span"), pl.first("is_abnormal").alias("is_abnormal")])
    )

    trace_entries_df = trace_entries_lf.collect()

    # Get entry span distribution for rare span calculation
    entry_span_counts = (
        trace_entries_df.group_by("entry_span")
        .agg(pl.len().alias("count"))
        .with_columns((pl.col("count") / total_traces).alias("proportion"))
    )

    rare_threshold = 0.05  # 5% threshold for rare spans
    rare_spans = entry_span_counts.filter(pl.col("proportion") < rare_threshold)["entry_span"].to_list()

    # Load detector conclusions if available
    detector_spans = set()
    conclusion_path = input_folder / "conclusion.parquet"
    if conclusion_path.exists():
        detector_df = pl.read_parquet(conclusion_path)
        if len(detector_df) > 0 and "SpanName" in detector_df.columns:
            # Parse detector span names
            detector_spans = set(detector_df["SpanName"].map_elements(extract_path, return_dtype=pl.String).to_list())

    # Join sampled traces with entry information
    sampled_trace_ids = set(sampled_df["trace_id"].to_list())
    sampled_traces_info = trace_entries_df.filter(pl.col("trace_id").is_in(sampled_trace_ids))

    # Calculate metrics
    total_entry_types = entry_span_counts.shape[0]
    sampled_entry_types = len(sampled_traces_info["entry_span"].unique())
    comprehensiveness = sampled_entry_types / total_entry_types if total_entry_types > 0 else 0.0

    # Calculate proportions
    anomaly_sampled = sampled_traces_info.filter(pl.col("is_abnormal"))
    proportion_anomaly = len(anomaly_sampled) / sampled_count if sampled_count > 0 else 0.0

    rare_sampled = sampled_traces_info.filter(pl.col("entry_span").is_in(rare_spans))
    proportion_rare = len(rare_sampled) / sampled_count if sampled_count > 0 else 0.0

    detector_sampled = sampled_traces_info.filter(pl.col("entry_span").is_in(list(detector_spans)))
    proportion_detector = len(detector_sampled) / sampled_count if sampled_count > 0 else 0.0

    return {
        "sampled_count": sampled_count,
        "total_traces": total_traces,
        "total_entry_types": total_entry_types,
        "sampled_entry_types": sampled_entry_types,
        "controllability": controllability,
        "comprehensiveness": comprehensiveness,
        "proportion_anomaly": proportion_anomaly,
        "proportion_rare": proportion_rare,
        "proportion_detector": proportion_detector,
        "actual_sampling_rate": actual_sampling_rate,
        "runtime_per_span_ns": runtime * 1e9 / total_traces if runtime and total_traces > 0 else None,
    }
