import polars as pl

from .data_prepare import Item


def aggregate(items: list[Item]) -> pl.DataFrame:
    if not items:
        return pl.DataFrame()

    data_rows = []

    for item in items:
        row = {
            "injection_id": item._injection.id,
            "injection_name": item._injection.injection_name,
            "fault_type": item.fault_type,
            "injected_service": item.injected_service,
            "is_pair": item.is_pair,
            "anomaly_degree": item.anomaly_degree,
            "workload": item.workload,
            # Data statistics
            "trace_count": item.trace_count,
            "duration_seconds": item.duration.total_seconds(),
            "qps": item.qps,
            "qpm": item.qpm,
            "service_count": len(item.service_names),
            "service_count_by_trace": len(item.service_names_by_trace),
            "service_coverage": item.service_coverage,
            # Log statistics
            "total_log_lines": sum(item.log_lines.values()),
            "log_services_count": len(item.log_lines),
            # Metric statistics
            "total_metric_count": sum(item.metric_count.values()),
            "unique_metrics": len(item.metric_count),
            # Trace depth statistics
            "avg_trace_length": (
                sum(length * count for length, count in item.trace_length.items()) / sum(item.trace_length.values())
                if item.trace_length
                else 0
            ),
            "max_trace_length": max(item.trace_length.keys()) if item.trace_length else 0,
            "min_trace_length": min(item.trace_length.keys()) if item.trace_length else 0,
        }

        for metric_name, metric_value in item.metrics.items():
            row[f"metric_{metric_name}"] = metric_value

        data_rows.append(row)

    df = pl.DataFrame(data_rows)

    return df


def get_summary_stats(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame()

    # Select numeric columns for statistics
    numeric_cols = [
        "trace_count",
        "duration_seconds",
        "qps",
        "qpm",
        "service_count",
        "service_count_by_trace",
        "service_coverage",
        "total_log_lines",
        "log_services_count",
        "total_metric_count",
        "unique_metrics",
        "avg_trace_length",
        "max_trace_length",
        "min_trace_length",
    ]

    # Filter existing columns
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]

    summary = df.select(existing_numeric_cols).describe()

    return summary


def get_groupby_stats(df: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
    if df.height == 0 or not group_cols:
        return pl.DataFrame()

    # Check if grouping columns exist
    existing_group_cols = [col for col in group_cols if col in df.columns]
    if not existing_group_cols:
        return pl.DataFrame()

    # Basic statistics
    stats = df.group_by(existing_group_cols).agg(
        [
            pl.len().alias("count"),
            pl.col("trace_count").mean().alias("avg_trace_count"),
            pl.col("duration_seconds").mean().alias("avg_duration_seconds"),
            pl.col("qps").mean().alias("avg_qps"),
            pl.col("service_count").mean().alias("avg_service_count"),
            pl.col("service_coverage").mean().alias("avg_service_coverage"),
            pl.col("total_log_lines").sum().alias("total_log_lines"),
            pl.col("total_metric_count").sum().alias("total_metric_count"),
        ]
    )

    return stats


def get_fault_type_stats(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0 or "fault_type" not in df.columns:
        return pl.DataFrame()

    metrics = [
        "trace_count",
        "duration_seconds",
        "qps",
        "service_count",
        "service_count_by_trace",
        "service_coverage",
        "total_log_lines",
        "log_services_count",
        "total_metric_count",
        "unique_metrics",
        "avg_trace_length",
        "max_trace_length",
        "min_trace_length",
    ]
    metrics = [m for m in metrics if m in df.columns]

    stats = df.group_by("fault_type").agg(
        [pl.len().alias("count"), *[pl.col(m).mean().alias(f"avg_{m}") for m in metrics]]
    )
    return stats
