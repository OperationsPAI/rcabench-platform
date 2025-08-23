import polars as pl

from .data_prepare import Item

FAULT_TYPE_MAPPING = {
    # Pod/容器级故障
    "PodKill": "Pod",
    "PodFailure": "Pod",
    "ContainerKill": "Pod",
    # 资源压力类故障
    "MemoryStress": "Resource",
    "CPUStress": "Resource",
    "JVMCPUStress": "Resource",
    "JVMMemoryStress": "Resource",
    # HTTP故障注入
    "HTTPRequestAbort": "HTTP",
    "HTTPResponseAbort": "HTTP",
    "HTTPRequestDelay": "HTTP",
    "HTTPResponseDelay": "HTTP",
    "HTTPResponseReplaceBody": "HTTP",
    "HTTPResponsePatchBody": "HTTP",
    "HTTPRequestReplacePath": "HTTP",
    "HTTPRequestReplaceMethod": "HTTP",
    "HTTPResponseReplaceCode": "HTTP",
    # DNS故障
    "DNSError": "DNS",
    "DNSRandom": "DNS",
    # 时间相关故障
    "TimeSkew": "Time",
    # 网络故障
    "NetworkDelay": "Network",
    "NetworkLoss": "Network",
    "NetworkDuplicate": "Network",
    "NetworkCorrupt": "Network",
    "NetworkBandwidth": "Network",
    "NetworkPartition": "Network",
    # JVM应用层故障
    "JVMLatency": "JVM",
    "JVMReturn": "JVM",
    "JVMException": "JVM",
    "JVMGarbageCollector": "JVM",
    "JVMMySQLLatency": "JVM",
    "JVMMySQLException": "JVM",
}


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
            "total_metric_count": sum(item.injection_metric_counts.values()),
            "unique_metrics": len(item.injection_metric_counts),
            # Trace depth statistics
            "avg_trace_length": (
                sum(length * count for length, count in item.trace_length.items()) / sum(item.trace_length.values())
                if item.trace_length
                else 0
            ),
            "max_trace_length": max(item.trace_length.keys()) if item.trace_length else 0,
            "min_trace_length": min(item.trace_length.keys()) if item.trace_length else 0,
        }

        for metric_name, metric_value in item.datapack_metric_values.items():
            row[f"datapack_metric_{metric_name}"] = metric_value

        for algo_name, metric in item.algo_metrics.items():
            row[f"algo_{algo_name}"] = metric.to_dict()

        data_rows.append(row)

    df = pl.DataFrame(data_rows)

    return df


def get_fault_type_stats(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0 or "fault_type" not in df.columns:
        return pl.DataFrame()

    df_with_category = df.with_columns(
        pl.col("fault_type")
        .map_elements(lambda x: FAULT_TYPE_MAPPING.get(x, "Unknown"), return_dtype=pl.Utf8)
        .alias("fault_category")
    )

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
    metrics = [m for m in metrics if m in df_with_category.columns]

    # Find datapack metric columns
    datapack_metrics = [col for col in df_with_category.columns if col.startswith("datapack_metric_")]

    # Find algorithm columns and extract algorithm metrics
    algo_cols = [col for col in df_with_category.columns if col.startswith("algo_")]

    agg_exprs = [pl.len().alias("count")]

    # Add basic metrics aggregation
    agg_exprs.extend([pl.col(m).mean().alias(f"avg_{m}") for m in metrics])

    # Add datapack metrics aggregation
    agg_exprs.extend([pl.col(dm).mean().alias(f"avg_{dm}") for dm in datapack_metrics])

    # Add algorithm metrics aggregation
    # Since algo columns contain dictionaries, we need to extract specific metrics
    for algo_col in algo_cols:
        # Extract top1, top3, top5, mrr from the algorithm dictionary
        for metric in ["top1", "top3", "top5", "mrr"]:
            agg_exprs.append(
                pl.col(algo_col)
                .map_elements(
                    lambda x, m=metric: x.get(m, 0.0) if isinstance(x, dict) else 0.0, return_dtype=pl.Float64
                )
                .mean()
                .alias(f"avg_{algo_col}_{metric}")
            )

    # Group by fault category instead of individual fault type
    stats = df_with_category.group_by("fault_category").agg(agg_exprs)
    return stats


def get_detailed_fault_type_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Get detailed statistics grouped by both fault_type and fault_category"""
    if df.height == 0 or "fault_type" not in df.columns:
        return pl.DataFrame()

    # Map fault types to fault categories using FAULT_TYPE_MAPPING
    df_with_category = df.with_columns(
        pl.col("fault_type")
        .map_elements(lambda x: FAULT_TYPE_MAPPING.get(x, "Unknown"), return_dtype=pl.Utf8)
        .alias("fault_category")
    )

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
    metrics = [m for m in metrics if m in df_with_category.columns]

    # Find datapack metric columns
    datapack_metrics = [col for col in df_with_category.columns if col.startswith("datapack_metric_")]

    # Find algorithm columns and extract algorithm metrics
    algo_cols = [col for col in df_with_category.columns if col.startswith("algo_")]

    agg_exprs = [pl.len().alias("count")]

    # Add basic metrics aggregation
    agg_exprs.extend([pl.col(m).mean().alias(f"avg_{m}") for m in metrics])

    # Add datapack metrics aggregation
    agg_exprs.extend([pl.col(dm).mean().alias(f"avg_{dm}") for dm in datapack_metrics])

    # Add algorithm metrics aggregation
    for algo_col in algo_cols:
        for metric in ["top1", "top3", "top5", "mrr"]:
            agg_exprs.append(
                pl.col(algo_col)
                .map_elements(
                    lambda x, m=metric: x.get(m, 0.0) if isinstance(x, dict) else 0.0, return_dtype=pl.Float64
                )
                .mean()
                .alias(f"avg_{algo_col}_{metric}")
            )

    # Group by both fault_category and fault_type
    stats = df_with_category.group_by(["fault_category", "fault_type"]).agg(agg_exprs)
    return stats


def get_groupby_stats(df: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
    if df.height == 0 or not group_cols:
        return pl.DataFrame()

    # Check if grouping columns exist
    existing_group_cols = [col for col in group_cols if col in df.columns]
    if not existing_group_cols:
        return pl.DataFrame()

    # Find datapack metric columns
    datapack_metrics = [col for col in df.columns if col.startswith("datapack_metric_")]

    # Find algorithm columns
    algo_cols = [col for col in df.columns if col.startswith("algo_")]

    # Basic statistics
    agg_exprs = [
        pl.len().alias("count"),
        pl.col("trace_count").mean().alias("avg_trace_count"),
        pl.col("duration_seconds").mean().alias("avg_duration_seconds"),
        pl.col("qps").mean().alias("avg_qps"),
        pl.col("service_count").mean().alias("avg_service_count"),
        pl.col("service_coverage").mean().alias("avg_service_coverage"),
        pl.col("total_log_lines").sum().alias("total_log_lines"),
        pl.col("total_metric_count").sum().alias("total_metric_count"),
    ]

    # Add datapack metrics aggregation
    agg_exprs.extend([pl.col(dm).mean().alias(f"avg_{dm}") for dm in datapack_metrics])

    # Add algorithm metrics aggregation
    for algo_col in algo_cols:
        for metric in ["top1", "top3", "top5", "mrr"]:
            agg_exprs.append(
                pl.col(algo_col)
                .map_elements(
                    lambda x, m=metric: x.get(m, 0.0) if isinstance(x, dict) else 0.0, return_dtype=pl.Float64
                )
                .mean()
                .alias(f"avg_{algo_col}_{metric}")
            )

    stats = df.group_by(existing_group_cols).agg(agg_exprs)

    return stats


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

    # Find datapack metric columns
    datapack_metrics = [col for col in df.columns if col.startswith("datapack_metric_")]

    # Filter existing columns
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    existing_datapack_cols = [col for col in datapack_metrics if col in df.columns]

    # Combine all numeric columns
    all_numeric_cols = existing_numeric_cols + existing_datapack_cols

    summary = df.select(all_numeric_cols).describe()

    return summary
