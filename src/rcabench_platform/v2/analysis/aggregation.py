from typing import Literal, TypedDict

import numpy as np
import polars as pl

from .data_prepare import Item


class CategoricalGroupSpec(TypedDict):
    type: Literal["categorical"]
    column: Literal["fault_type", "fault_category", "injected_service", "is_pair", "anomaly_degree", "SDD@1"]


class NumericBinsGroupSpec(TypedDict):
    type: Literal["numeric_bins"]
    column: Literal[
        "SDD@1",
        "CPL",
        "RootServiceDegree",
    ]
    bins: list[float] | None


GroupSpec = CategoricalGroupSpec | NumericBinsGroupSpec

FAULT_TYPE_MAPPING = {
    # Pod/container-level faults
    "PodKill": "Pod",
    "PodFailure": "Pod",
    "ContainerKill": "Pod",
    # resource stress
    "MemoryStress": "Resource",
    "CPUStress": "Resource",
    "JVMCPUStress": "Resource",
    "JVMMemoryStress": "Resource",
    # HTTP faults
    "HTTPRequestAbort": "HTTP",
    "HTTPResponseAbort": "HTTP",
    "HTTPRequestDelay": "HTTP",
    "HTTPResponseDelay": "HTTP",
    "HTTPResponseReplaceBody": "HTTP",
    "HTTPResponsePatchBody": "HTTP",
    "HTTPRequestReplacePath": "HTTP",
    "HTTPRequestReplaceMethod": "HTTP",
    "HTTPResponseReplaceCode": "HTTP",
    # DNS
    "DNSError": "DNS",
    "DNSRandom": "DNS",
    # time
    "TimeSkew": "Time",
    # network faults
    "NetworkDelay": "Network",
    "NetworkLoss": "Network",
    "NetworkDuplicate": "Network",
    "NetworkCorrupt": "Network",
    "NetworkBandwidth": "Network",
    "NetworkPartition": "Network",
    # JVM application-level
    "JVMLatency": "JVM",
    "JVMReturn": "JVM",
    "JVMException": "JVM",
    "JVMGarbageCollector": "JVM",
    "JVMMySQLLatency": "JVM",
    "JVMMySQLException": "JVM",
}


def aggregate(items: list[Item], group_specs: list[GroupSpec] | None = None) -> pl.DataFrame:
    if not items:
        return pl.DataFrame()

    data_rows = []

    for item in items:
        assert "SDD@1" in item.datapack_metric_values
        assert "CPL" in item.datapack_metric_values
        assert "RootServiceDegree" in item.datapack_metric_values
        row = {
            "injection_id": item._injection.id,
            "injection_name": item._injection.injection_name,
            "fault_type": item.fault_type,
            "fault_category": FAULT_TYPE_MAPPING.get(item.fault_type, "Unknown"),
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
            "SDD@1": item.datapack_metric_values.get("SDD@1"),
            "CPL": item.datapack_metric_values.get("CPL"),
            "RootServiceDegree": item.datapack_metric_values.get("RootServiceDegree"),
        }

        for metric_name, metric_value in item.datapack_metric_values.items():
            row[f"datapack_metric_{metric_name}"] = metric_value

        for algo_name, metric in item.algo_metrics.items():
            row[f"algo_{algo_name}"] = metric.to_dict()

        data_rows.append(row)

    df = pl.DataFrame(data_rows)

    if group_specs:
        df = _add_grouping_columns(df, group_specs)

    return df


def get_stats_by_group(df: pl.DataFrame, group_specs: list[GroupSpec] | None = None) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame()

    if group_specs is None:
        group_cols = [col for col in df.columns if col.startswith("group_")]
        if not group_cols:
            return pl.DataFrame()
    else:
        group_cols = []
        for spec in group_specs:
            group_col_name = f"group_{spec['column']}"
            group_cols.append(group_col_name)

    missing_cols = [col for col in group_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing columns {missing_cols}, skipping them")
        group_cols = [col for col in group_cols if col in df.columns]

    if not group_cols:
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
        "SDD@1",
        "CPL",
        "RootServiceDegree",
    ]
    metrics = [m for m in metrics if m in df.columns]

    # Find datapack metric columns
    datapack_metrics = [col for col in df.columns if col.startswith("datapack_metric_")]

    # Find algorithm columns and extract algorithm metrics
    algo_cols = [col for col in df.columns if col.startswith("algo_")]

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

    stats = df.group_by(group_cols).agg(agg_exprs)
    return stats


def _add_grouping_columns(df: pl.DataFrame, group_specs: list[GroupSpec]) -> pl.DataFrame:
    for spec in group_specs:
        group_col_name = f"group_{spec['column']}"

        if spec["type"] == "categorical":
            if spec["column"] in df.columns:
                df = df.with_columns(pl.col(spec["column"]).cast(pl.String).alias(group_col_name))
        elif spec["type"] == "numeric_bins":
            if spec["column"] in df.columns:
                df = _add_numeric_bins(df, spec["column"], spec.get("bins"), group_col_name)

    return df


def _add_numeric_bins(
    df: pl.DataFrame,
    numeric_col: str,
    bins: list[float] | None = None,
    bin_col_name: str = "numeric_bin",
) -> pl.DataFrame:
    if df.height == 0 or numeric_col not in df.columns:
        return df

    # Filter out null values for binning calculation but preserve original df structure
    df_filtered = df.filter(pl.col(numeric_col).is_not_null())

    if df_filtered.height == 0:
        return df

    # Create bins
    if bins is None:
        # Automatic binning - create 5 bins based on data distribution
        min_val = df_filtered[numeric_col].min()
        max_val = df_filtered[numeric_col].max()

        if min_val is None or max_val is None:
            return df

        try:
            # Handle different data types
            if isinstance(min_val, (int, float)):
                min_val_float = float(min_val)
            else:
                min_val_float = float(str(min_val))

            if isinstance(max_val, (int, float)):
                max_val_float = float(max_val)
            else:
                max_val_float = float(str(max_val))
        except (ValueError, TypeError):
            return df

        if min_val_float == max_val_float:
            bins = [min_val_float - 0.5, min_val_float + 0.5]
        else:
            bins = np.linspace(min_val_float, max_val_float, 6).tolist()  # 5 bins

    if bins is None or len(bins) < 2:
        return df

    # Create bin labels
    bin_labels = []
    for i in range(len(bins) - 1):
        if i == len(bins) - 2:  # Last bin - include upper bound
            bin_labels.append(f"[{bins[i]:.1f}, {bins[i + 1]:.1f}]")
        else:
            bin_labels.append(f"[{bins[i]:.1f}, {bins[i + 1]:.1f})")

    def assign_bin(value):
        """Assign a value to the appropriate bin."""
        if value is None:
            return None

        try:
            # Convert to float for comparison
            if isinstance(value, (int, float)):
                val_float = float(value)
            else:
                val_float = float(str(value))
        except (ValueError, TypeError):
            return None

        for i in range(len(bins) - 1):
            if i == len(bins) - 2:  # Last bin - include upper bound
                if bins[i] <= val_float <= bins[i + 1]:
                    return bin_labels[i]
            else:
                if bins[i] <= val_float < bins[i + 1]:
                    return bin_labels[i]

        return None

    # Add bin column to original dataframe (not just filtered one)
    df_with_bins = df.with_columns(
        pl.col(numeric_col).map_elements(assign_bin, return_dtype=pl.String).alias(bin_col_name)
    )

    return df_with_bins
