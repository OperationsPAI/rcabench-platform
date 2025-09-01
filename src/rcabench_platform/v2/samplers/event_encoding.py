"""
Event Coverage Module for Trace Sampling

Encodes traces and logs into events and calculates coverage based on event pairs.
Simplified from original implementation, focusing on core event encoding performance.
"""

import datetime
import math
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import polars as pl

from ..logging import logger
from ..utils.serde import load_json


class EventType(Enum):
    """Event types for trace encoding"""

    SPAN_START = "span_start"
    SPAN_END = "span_end"
    STATUS_ERROR = "status_error"
    PERFORMANCE_DEGRADATION = "perf_degradation"
    LOG = "log"


@dataclass
class Event:
    """Event structure for trace encoding"""

    event_type: EventType
    event_id: int
    timestamp: float
    trace_id: str
    span_id: str | None = None


class EventIDManager:
    """Manages unique integer IDs for events"""

    def __init__(self):
        # ID ranges
        self.SPAN_NAME_START = 1
        self.SPAN_NAME_END = 10000
        self.SPECIAL_EVENT_START = 10001
        self.LOG_TEMPLATE_START = 20001

        # Current counters
        self.span_name_counter = self.SPAN_NAME_START
        self.special_event_counter = self.SPECIAL_EVENT_START
        self.log_template_counter = self.LOG_TEMPLATE_START

        # Mappings
        self.span_name_to_id: dict[str, int] = {}
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
            if service_span_name not in self.span_name_to_id:
                if self.span_name_counter <= self.SPAN_NAME_END:
                    self.span_name_to_id[service_span_name] = self.span_name_counter
                    self.span_name_counter += 1

        logger.debug(f"Assigned {len(self.span_name_to_id)} span name IDs")

    def get_span_event_id(self, service_span_name: str) -> int:
        """Get event ID for service span name combination"""
        return self.span_name_to_id.get(service_span_name, self.SPAN_NAME_END)

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
    ) -> list[Event]:
        """Encode a single trace into events"""
        events = []

        # Extract span events
        for row in trace_spans_df.iter_rows(named=True):
            service_span_name = f"{row['service_name']}_{row['span_name']}"
            span_start_id = self.event_manager.get_span_event_id(service_span_name)
            span_end_id = span_start_id  # Use same ID for start/end for simplicity

            timestamp = row["time"].timestamp() if hasattr(row["time"], "timestamp") else float(row["time"])

            # Span start event
            events.append(
                Event(
                    event_type=EventType.SPAN_START,
                    event_id=span_start_id,
                    timestamp=timestamp,
                    trace_id=row["trace_id"],
                    span_id=row["span_id"],
                )
            )

            # Status error event
            if row.get("attr.status_code") == "Error":
                error_id = self.event_manager.get_special_event_id("status_error")
                events.append(
                    Event(
                        event_type=EventType.STATUS_ERROR,
                        event_id=error_id,
                        timestamp=timestamp + 0.001,  # Slightly after span start
                        trace_id=row["trace_id"],
                        span_id=row["span_id"],
                    )
                )

            # Performance degradation event
            duration = row.get("duration", 0)
            p90_threshold = self.performance_thresholds.get(service_span_name)
            if p90_threshold and duration > p90_threshold:
                perf_id = self.event_manager.get_special_event_id("perf_degradation")
                events.append(
                    Event(
                        event_type=EventType.PERFORMANCE_DEGRADATION,
                        event_id=perf_id,
                        timestamp=timestamp + 0.002,  # After status error
                        trace_id=row["trace_id"],
                        span_id=row["span_id"],
                    )
                )

            # Span end event
            events.append(
                Event(
                    event_type=EventType.SPAN_END,
                    event_id=span_end_id,
                    timestamp=timestamp + (duration / 1e9 if duration else 0.01),  # Convert ns to seconds
                    trace_id=row["trace_id"],
                    span_id=row["span_id"],
                )
            )

        # Extract log events
        if trace_logs_df is not None and len(trace_logs_df) > 0:
            # Filter out noisy services
            filtered_logs = trace_logs_df.filter(pl.col("service_name") != "ts-ui-dashboard")

            for row in filtered_logs.iter_rows(named=True):
                template_id = row.get("attr.template_id")
                if template_id is not None:
                    log_event_id = self.event_manager.get_log_event_id(str(template_id))
                    timestamp = row["time"].timestamp() if hasattr(row["time"], "timestamp") else float(row["time"])

                    events.append(
                        Event(
                            event_type=EventType.LOG,
                            event_id=log_event_id,
                            timestamp=timestamp,
                            trace_id=row["trace_id"],
                            span_id=row.get("span_id"),
                        )
                    )

        # Sort events by timestamp
        events.sort(key=lambda e: e.timestamp)
        return events

    def extract_event_pairs(self, events: list[Event]) -> set[tuple[int, int]]:
        """Extract consecutive event pairs (2-grams) from event sequence"""
        pairs = set()

        if len(events) < 2:
            return pairs

        # Create pairs from consecutive events
        for i in range(len(events) - 1):
            curr_event = events[i]
            next_event = events[i + 1]
            pairs.add((curr_event.event_id, next_event.event_id))

        return pairs


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
        Dictionary containing event coverage metrics
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

    # Process each trace
    for (trace_id,), trace_df in trace_groups.items():
        if not trace_id:
            continue

        # Get logs for this trace
        trace_logs = log_groups.get((trace_id,), pl.DataFrame())

        # Encode events for this trace
        events = encoder.encode_trace_events(trace_df, trace_logs)

        # Extract event pairs
        event_pairs = encoder.extract_event_pairs(events)

        # Add to all pairs
        all_event_pairs.update(event_pairs)

        # Add to sampled pairs if this trace was sampled
        if trace_id in sampled_trace_ids:
            sampled_event_pairs.update(event_pairs)

    # Calculate coverage metrics
    total_event_pairs = len(all_event_pairs)
    sampled_event_pairs_count = len(sampled_event_pairs)

    event_coverage = sampled_event_pairs_count / total_event_pairs if total_event_pairs > 0 else 0.0

    logger.info(f"Event coverage: {sampled_event_pairs_count}/{total_event_pairs} = {event_coverage:.4f}")

    return {
        "total_event_pairs": total_event_pairs,
        "sampled_event_pairs": sampled_event_pairs_count,
        "event_coverage": event_coverage,
    }
