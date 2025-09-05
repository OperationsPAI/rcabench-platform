"""
Event Coverage Module for Trace Sampling

Encodes traces and logs into events and calculates coverage based on event pairs.
Simplified from original implementation, focusing on core event encoding performance.
"""

import datetime
import math
from collections import defaultdict
from enum import Enum
from pathlib import Path

import polars as pl

from ..logging import logger, timeit
from ..utils.serde import load_json


class EventType(Enum):
    """Event types for trace encoding (kept for reference)"""

    SPAN_START = "span_start"
    SPAN_END = "span_end"
    STATUS_ERROR = "status_error"
    PERFORMANCE_DEGRADATION = "perf_degradation"
    LOG = "log"


class EventIDManager:
    """Manages unique integer IDs for events"""

    def __init__(self):
        # ID ranges
        self.SPAN_START_BEGIN = 1
        self.SPAN_START_END = 5000
        self.SPAN_END_BEGIN = 5001
        self.SPAN_END_END = 10000
        self.SPECIAL_EVENT_START = 10001
        self.LOG_TEMPLATE_START = 20001

        # Current counters
        self.span_start_counter = self.SPAN_START_BEGIN
        self.span_end_counter = self.SPAN_END_BEGIN
        self.special_event_counter = self.SPECIAL_EVENT_START
        self.log_template_counter = self.LOG_TEMPLATE_START

        # Mappings
        self.span_start_to_id: dict[str, int] = {}
        self.span_end_to_id: dict[str, int] = {}
        self.special_event_to_id: dict[str, int] = {}
        self.log_template_to_id: dict[str, int] = {}

        # Initialize fixed special events
        self.special_event_to_id["status_error"] = self.special_event_counter
        self.special_event_counter += 1
        self.special_event_to_id["perf_degradation"] = self.special_event_counter
        self.special_event_counter += 1

    def extract_span_names_from_traces(self, traces_df: pl.DataFrame) -> None:
        """Extract unique service_name + span_name combinations and assign IDs"""
        unique_combinations = (
            traces_df.select([pl.concat_str(["service_name", "span_name"], separator="_").alias("service_span_name")])
            .unique()
            .to_series()
            .to_list()
        )

        logger.debug(f"Found {len(unique_combinations)} unique service_span_name combinations")

        for service_span_name in unique_combinations:
            # Assign span start ID
            if service_span_name not in self.span_start_to_id:
                if self.span_start_counter <= self.SPAN_START_END:
                    self.span_start_to_id[service_span_name] = self.span_start_counter
                    self.span_start_counter += 1

            # Assign span end ID
            if service_span_name not in self.span_end_to_id:
                if self.span_end_counter <= self.SPAN_END_END:
                    self.span_end_to_id[service_span_name] = self.span_end_counter
                    self.span_end_counter += 1

        logger.debug(
            f"Assigned {len(self.span_start_to_id)} span start IDs and {len(self.span_end_to_id)} span end IDs"
        )

    def get_span_start_id(self, service_span_name: str) -> int:
        """Get event ID for span start"""
        return self.span_start_to_id.get(service_span_name, self.SPAN_START_END)

    def get_span_end_id(self, service_span_name: str) -> int:
        """Get event ID for span end"""
        return self.span_end_to_id.get(service_span_name, self.SPAN_END_END)

    def get_special_event_id(self, event_type: str) -> int:
        """Get event ID for special events"""
        return self.special_event_to_id.get(event_type, self.special_event_counter - 1)

    def get_log_event_id(self, template_id: str) -> int:
        """Get event ID for log template"""
        if template_id not in self.log_template_to_id:
            self.log_template_to_id[template_id] = self.log_template_counter
            self.log_template_counter += 1
        return self.log_template_to_id[template_id]


class EventEncoder:
    """Encodes traces and logs into events for coverage analysis"""

    def __init__(self, event_manager: EventIDManager):
        self.event_manager = event_manager
        self.performance_thresholds: dict[str, float] = {}

    def load_inject_time(self, input_folder: Path) -> datetime.datetime:
        """Load injection time from env.json"""
        env = load_json(path=input_folder / "env.json")

        normal_start = int(env["NORMAL_START"])
        normal_end = int(env["NORMAL_END"])
        abnormal_start = int(env["ABNORMAL_START"])
        abnormal_end = int(env["ABNORMAL_END"])

        assert normal_start < normal_end <= abnormal_start < abnormal_end

        if normal_end < abnormal_start:
            inject_time = int(math.ceil(normal_end + abnormal_start) / 2)
        else:
            inject_time = abnormal_start

        inject_time = datetime.datetime.fromtimestamp(inject_time, tz=datetime.timezone.utc)
        logger.debug(f"inject_time=`{inject_time}`")

        return inject_time

    def load_performance_thresholds(self, input_folder: Path) -> None:
        """Load performance thresholds from metrics_sli.parquet using only normal phase data"""
        try:
            metrics_sli_path = input_folder / "metrics_sli.parquet"
            if not metrics_sli_path.exists():
                logger.warning("metrics_sli.parquet not found, performance degradation detection disabled")
                return

            metrics_df = pl.read_parquet(metrics_sli_path)

            # Filter to only normal phase data for unbiased threshold calculation
            try:
                inject_time = self.load_inject_time(input_folder)
                metrics_df = metrics_df.filter(pl.col("time") < inject_time)
                logger.debug(
                    f"Filtered metrics_sli to {len(metrics_df)} normal phase records for threshold calculation"
                )
            except Exception as e:
                logger.warning(f"Failed to load inject time, using all metrics_sli data: {e}")

            # Calculate p90 thresholds per service_name + span_name using normal phase data only
            thresholds_df = (
                metrics_df.group_by(["service_name", "span_name"])
                .agg([pl.col("duration_p90").mean().alias("p90_threshold")])
                .with_columns([pl.concat_str(["service_name", "span_name"], separator="_").alias("service_span_name")])
            )

            for row in thresholds_df.iter_rows(named=True):
                service_span_name = row["service_span_name"]
                p90_threshold = row["p90_threshold"]
                if p90_threshold is not None:
                    # Convert to nanoseconds (metrics_sli is in ms, traces are in ns)
                    self.performance_thresholds[service_span_name] = p90_threshold * 1_000_000

            logger.debug(f"Loaded performance thresholds for {len(self.performance_thresholds)} span types")

        except Exception as e:
            logger.warning(f"Failed to load performance thresholds: {e}")

    def encode_trace_events(
        self, trace_spans_df: pl.DataFrame, trace_logs_df: pl.DataFrame | None = None
    ) -> set[tuple[int, int]]:
        """Encode a single trace into event ID sequence respecting span hierarchy"""

        # First, find root span using DataFrame filtering (more efficient)
        # Root span must be loadgenerator service with null/empty parent_span_id
        root_spans_df = trace_spans_df.filter(
            (pl.col("service_name") == "loadgenerator")
            & (pl.col("parent_span_id").is_null() | (pl.col("parent_span_id") == ""))
        )

        # If no valid root span found, skip this trace
        if root_spans_df.height == 0:
            logger.debug("No root span found, skipping trace")
            return set()

        # Get the first (and only) root span ID directly (for validation only)
        # We don't use root_span_id in the new approach since we process all spans

        # Build span hierarchy
        spans_data = {}
        children_map = defaultdict(list)

        for row in trace_spans_df.iter_rows(named=True):
            span_id = row["span_id"]
            parent_id = row.get("parent_span_id")

            spans_data[span_id] = row

            if parent_id and parent_id != "":
                children_map[parent_id].append(span_id)

        # Prepare log events by span
        log_events_by_span = defaultdict(list)
        if trace_logs_df is not None and len(trace_logs_df) > 0:
            for row in trace_logs_df.iter_rows(named=True):
                template_id = row.get("attr.template_id")
                if template_id is not None:
                    log_event_id = self.event_manager.get_log_event_id(str(template_id))
                    timestamp = row["time"].timestamp() if hasattr(row["time"], "timestamp") else float(row["time"])

                    span_id = row.get("span_id")
                    if span_id:
                        log_events_by_span[span_id].append((log_event_id, timestamp))

            # Sort log events within each span by timestamp
            for span_id in log_events_by_span:
                log_events_by_span[span_id].sort(key=lambda x: x[1])

        # Generate event pairs more efficiently without recursion
        all_event_pairs = set()

        # 1. Generate span internal event pairs for each span
        for span_id, span_data in spans_data.items():
            service_span_name = f"{span_data['service_name']}_{span_data['span_name']}"
            span_start_id = self.event_manager.get_span_start_id(service_span_name)
            span_end_id = self.event_manager.get_span_end_id(service_span_name)

            # Build internal event sequence for this span
            span_events = [span_start_id]  # Start with span start

            # Add log events (sorted by timestamp)
            if span_id in log_events_by_span:
                for log_event_id, _ in log_events_by_span[span_id]:
                    span_events.append(log_event_id)

            # Add status error event (if applicable)
            if span_data.get("attr.status_code") == "Error":
                error_id = self.event_manager.get_special_event_id("status_error")
                span_events.append(error_id)

            # Add performance degradation event (if applicable)
            duration = span_data.get("duration", 0)
            p90_threshold = self.performance_thresholds.get(service_span_name)
            if p90_threshold and duration > p90_threshold:
                perf_id = self.event_manager.get_special_event_id("perf_degradation")
                span_events.append(perf_id)

            # End with span end (different ID from start)
            span_events.append(span_end_id)

            # Extract internal pairs for this span
            span_pairs = self.extract_event_pairs(span_events)
            all_event_pairs.update(span_pairs)

        # 2. Generate span relation event pairs (parent end -> child start)
        for parent_id, children in children_map.items():
            if parent_id in spans_data:
                parent_data = spans_data[parent_id]
                parent_service_span = f"{parent_data['service_name']}_{parent_data['span_name']}"
                parent_end_id = self.event_manager.get_span_end_id(parent_service_span)

                # Sort children by timestamp for deterministic ordering
                children.sort(key=lambda cid: spans_data[cid]["time"])

                for child_id in children:
                    if child_id in spans_data:
                        child_data = spans_data[child_id]
                        child_service_span = f"{child_data['service_name']}_{child_data['span_name']}"
                        child_start_id = self.event_manager.get_span_start_id(child_service_span)

                        # Add parent_end -> child_start pair
                        all_event_pairs.add((parent_end_id, child_start_id))

        return all_event_pairs

    def extract_event_pairs(self, event_ids: list[int]) -> set[tuple[int, int]]:
        """Extract consecutive event pairs (2-grams) from event ID sequence using optimized zip approach"""
        if len(event_ids) < 2:
            return set()

        # Most efficient approach based on performance testing
        return set(zip(event_ids[:-1], event_ids[1:]))


@timeit(log_args=False)
def calculate_event_coverage(
    traces_df: pl.DataFrame, logs_df: pl.DataFrame | None, sampled_trace_ids: set[str], input_folder
) -> dict[str, float]:
    """
    Calculate event coverage metrics for sampled traces.

    Args:
        traces_df: All traces data
        logs_df: All logs data (optional)
        sampled_trace_ids: Set of sampled trace IDs
        input_folder: Path to input folder for loading metrics_sli

    Returns:
        Dictionary containing event coverage metrics including Shannon entropy and benefit-cost ratio
    """
    logger.info("Calculating event coverage metrics...")

    # Initialize event manager and encoder
    event_manager = EventIDManager()
    encoder = EventEncoder(event_manager)

    # Extract span names and load performance thresholds
    event_manager.extract_span_names_from_traces(traces_df)
    encoder.load_performance_thresholds(input_folder)

    # Group traces by trace_id
    trace_groups = traces_df.partition_by("trace_id", as_dict=True)

    # Group logs by trace_id if available
    log_groups = {}
    if logs_df is not None:
        log_groups = logs_df.partition_by("trace_id", as_dict=True)

    logger.info(f"Processing {len(trace_groups)} traces for event coverage")

    all_event_pairs = set()
    sampled_event_pairs = set()

    # Track unique trace patterns for unique trace coverage
    all_trace_patterns = set()
    sampled_trace_patterns = set()

    # For Shannon entropy calculation - track trace pattern counts in sampled data
    sampled_pattern_counts = {}

    # Process each trace
    for (trace_id,), trace_df in trace_groups.items():
        if not trace_id:
            continue

        # Get logs for this trace
        trace_logs = log_groups.get((trace_id,), pl.DataFrame())

        # Encode events for this trace and get event pairs directly
        event_pairs = encoder.encode_trace_events(trace_df, trace_logs)

        # Add to all pairs
        all_event_pairs.update(event_pairs)

        # Add unique trace pattern (frozenset of event pairs for hashability)
        if event_pairs:  # Only add non-empty patterns
            trace_pattern = frozenset(event_pairs)
            all_trace_patterns.add(trace_pattern)

            # Add to sampled patterns if this trace was sampled
            if trace_id in sampled_trace_ids:
                sampled_trace_patterns.add(trace_pattern)

                # Count pattern occurrences for Shannon entropy
                sampled_pattern_counts[trace_pattern] = sampled_pattern_counts.get(trace_pattern, 0) + 1

        # Add to sampled pairs if this trace was sampled
        if trace_id in sampled_trace_ids:
            sampled_event_pairs.update(event_pairs)

    # Calculate coverage metrics
    total_event_pairs = len(all_event_pairs)
    sampled_event_pairs_count = len(sampled_event_pairs)

    # Calculate unique trace coverage metrics
    total_unique_traces = len(all_trace_patterns)
    sampled_unique_traces = len(sampled_trace_patterns)

    event_coverage = sampled_event_pairs_count / total_event_pairs if total_event_pairs > 0 else 0.0
    unique_trace_coverage = sampled_unique_traces / total_unique_traces if total_unique_traces > 0 else 0.0

    # Calculate Shannon entropy of trace pattern distribution in sampled data
    shannon_entropy = 0.0
    if len(sampled_pattern_counts) > 1:  # Need at least 2 different patterns
        total_sampled_traces = sum(sampled_pattern_counts.values())

        for count in sampled_pattern_counts.values():
            if count > 0:  # Avoid log(0)
                p_i = count / total_sampled_traces
                shannon_entropy -= p_i * math.log2(p_i)

    # Calculate benefit-cost ratio
    benefit_cost_ratio = 0.0
    actual_sample_count = len(sampled_trace_ids)
    if actual_sample_count > 0:
        benefit_cost_ratio = sampled_unique_traces / actual_sample_count

    # Calculate intra-sample average dissimilarity
    sampled_patterns_list = list(sampled_trace_patterns)
    intra_sample_dissimilarity = calculate_intra_sample_dissimilarity(sampled_patterns_list)

    logger.info(f"Event coverage: {sampled_event_pairs_count}/{total_event_pairs} = {event_coverage:.4f}")
    logger.info(f"Unique trace coverage: {sampled_unique_traces}/{total_unique_traces} = {unique_trace_coverage:.4f}")
    logger.info(f"Shannon entropy: {shannon_entropy:.4f} (from {len(sampled_pattern_counts)} pattern types)")
    logger.info(f"Benefit-cost ratio: {benefit_cost_ratio:.4f} ({sampled_unique_traces}/{actual_sample_count})")
    logger.info(f"Intra-sample dissimilarity: {intra_sample_dissimilarity:.4f}")

    return {
        "total_event_pairs": total_event_pairs,
        "sampled_event_pairs": sampled_event_pairs_count,
        "event_coverage": event_coverage,
        "total_unique_traces": total_unique_traces,
        "sampled_unique_traces": sampled_unique_traces,
        "unique_trace_coverage": unique_trace_coverage,
        "shannon_entropy": shannon_entropy,
        "benefit_cost_ratio": benefit_cost_ratio,
        "intra_sample_dissimilarity": intra_sample_dissimilarity,
    }


def calculate_intra_sample_dissimilarity(sampled_trace_patterns: list[frozenset]) -> float:
    """
    Calculate intra-sample average dissimilarity for sampled traces.

    This measures how dissimilar traces are to each other within the sampled set,
    which is essential for evaluating diversity-aware sampling algorithms like DPP.

    Args:
        sampled_trace_patterns: List of trace patterns (each as frozenset of event pairs)

    Returns:
        Average dissimilarity score [0, 1] where 1 = maximum diversity
    """
    n = len(sampled_trace_patterns)

    if n <= 1:
        return 0.0  # No diversity possible with 0 or 1 trace

    total_dissimilarity = 0.0
    pair_count = 0

    # Calculate pairwise Jaccard dissimilarity for all pairs
    for i in range(n):
        for j in range(i + 1, n):
            trace_i = sampled_trace_patterns[i]
            trace_j = sampled_trace_patterns[j]

            # Calculate Jaccard similarity
            intersection = len(trace_i & trace_j)
            union = len(trace_i | trace_j)

            if union == 0:
                jaccard_similarity = 0.0  # Both traces empty
            else:
                jaccard_similarity = intersection / union

            # Convert to dissimilarity
            dissimilarity = 1.0 - jaccard_similarity
            total_dissimilarity += dissimilarity
            pair_count += 1

    # Return average dissimilarity
    average_dissimilarity = total_dissimilarity / pair_count if pair_count > 0 else 0.0

    logger.debug(f"Intra-sample dissimilarity: {average_dissimilarity:.4f} from {n} traces ({pair_count} pairs)")

    return average_dissimilarity


def calculate_anomaly_score_per_trace(
    traces_df: pl.DataFrame,
    logs_df: pl.DataFrame | None,
    sampled_trace_ids: set[str],
    input_folder: Path,
) -> dict[str, float]:
    """
    Calculate average anomaly score per trace for sampled traces.

    Optimized version using vectorized operations for better performance.

    Anomaly score calculation follows DPP-like scoring:
    - Error spans: +5 per error span
    - Performance degradation: +1/2/3 based on P90 ratio (1.5x/3x/5x)
    - Log level score: WARN +1, ERROR/SEVERE +2 each

    Args:
        traces_df: All traces data
        logs_df: All logs data (optional)
        sampled_trace_ids: Set of sampled trace IDs
        input_folder: Path to input folder for loading metrics_sli thresholds

    Returns:
        Dictionary with 'avg_anomaly_score' key
    """
    logger.info("Calculating average anomaly score per trace...")

    if not sampled_trace_ids:
        return {"avg_anomaly_score": 0.0}

    # Initialize event manager and encoder for performance thresholds
    event_manager = EventIDManager()
    encoder = EventEncoder(event_manager)

    # Load performance thresholds
    encoder.load_performance_thresholds(input_folder)

    # Filter to only sampled traces
    sampled_traces_df = traces_df.filter(pl.col("trace_id").is_in(sampled_trace_ids))

    # Step 1: Batch extract root spans info for all sampled traces
    root_spans_df = sampled_traces_df.filter(
        (pl.col("service_name") == "loadgenerator")
        & (pl.col("parent_span_id").is_null() | (pl.col("parent_span_id") == ""))
    ).select(["trace_id", "span_name", (pl.col("duration") / 1_000_000.0).alias("duration_ms")])

    if root_spans_df.is_empty():
        return {"avg_anomaly_score": 0.0}

    # Step 2: Batch calculate performance scores using vectorized operations
    # Create P90 threshold lookup using expression
    perf_conditions = pl.lit(0.0)  # Default performance score

    for span_pattern, threshold_ns in encoder.performance_thresholds.items():
        threshold_ms = threshold_ns / 1_000_000.0

        # Build condition for this threshold pattern
        pattern_condition = (
            pl.col("span_name").str.contains(span_pattern) | pl.lit(span_pattern).str.contains(pl.col("span_name"))
        ) & (pl.col("duration_ms") > threshold_ms)

        ratio = pl.col("duration_ms") / threshold_ms
        perf_score = (
            pl.when(ratio >= 5.0)
            .then(pl.lit(3.0))
            .when(ratio >= 3.0)
            .then(pl.lit(2.0))
            .when(ratio >= 1.5)
            .then(pl.lit(1.0))
            .otherwise(pl.lit(0.0))
        )

        # Update performance conditions
        perf_conditions = pl.when(pattern_condition).then(perf_score).otherwise(perf_conditions)

    # Add performance scores to root spans
    root_spans_with_perf = root_spans_df.with_columns([perf_conditions.alias("perf_score")])

    # Step 3: Batch calculate error span counts per trace
    error_counts = (
        sampled_traces_df.filter(pl.col("attr.status_code") == "Error")
        .group_by("trace_id")
        .agg([pl.len().alias("error_count")])
    )

    # Step 4: Batch calculate log scores per trace if logs available
    log_scores_df = None
    if logs_df is not None and not logs_df.is_empty():
        sampled_logs_df = logs_df.filter(pl.col("trace_id").is_in(sampled_trace_ids))

        if not sampled_logs_df.is_empty():
            log_scores_df = (
                sampled_logs_df.with_columns(
                    [
                        pl.when(pl.col("level").is_in(["WARNING", "WARN"]))
                        .then(pl.lit(1))
                        .when(pl.col("level").is_in(["ERROR", "SEVERE"]))
                        .then(pl.lit(2))
                        .otherwise(pl.lit(0))
                        .alias("log_score")
                    ]
                )
                .group_by("trace_id")
                .agg([pl.col("log_score").sum().alias("total_log_score")])
            )

    # Step 5: Join all components and calculate final anomaly scores
    anomaly_df = root_spans_with_perf

    # Left join with error counts (default 0 if no errors)
    anomaly_df = anomaly_df.join(error_counts, on="trace_id", how="left").with_columns(
        [pl.col("error_count").fill_null(0)]
    )

    # Left join with log scores (default 0 if no logs)
    if log_scores_df is not None:
        anomaly_df = anomaly_df.join(log_scores_df, on="trace_id", how="left").with_columns(
            [pl.col("total_log_score").fill_null(0.0)]
        )
    else:
        anomaly_df = anomaly_df.with_columns([pl.lit(0.0).alias("total_log_score")])

    # Step 6: Calculate final anomaly scores using vectorized operations
    final_scores = anomaly_df.with_columns(
        [(pl.col("error_count") * 5.0 + pl.col("perf_score") + pl.col("total_log_score")).alias("anomaly_score")]
    )

    # Calculate average anomaly score
    avg_anomaly_score = final_scores.select(pl.col("anomaly_score").mean()).item()
    avg_anomaly_score = float(avg_anomaly_score or 0.0)

    logger.debug(f"Average anomaly score: {avg_anomaly_score:.4f} from {len(final_scores)} traces")

    return {"avg_anomaly_score": avg_anomaly_score}
