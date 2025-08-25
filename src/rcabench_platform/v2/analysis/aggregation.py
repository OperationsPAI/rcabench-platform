import duckdb
import numpy as np
import polars as pl

from ..logging import logger
from .data_prepare import Item

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


def aggregate(items: list[Item]) -> pl.DataFrame:
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

    return df


class DuckDBAggregator:
    def __init__(self, df: pl.DataFrame):
        self.conn = duckdb.connect(":memory:")
        processed_df = self._flatten_algo_columns(df)
        self.conn.register("data", processed_df.to_arrow())

    def print_schema(self) -> None:
        try:
            schema_result = self.conn.execute("DESCRIBE data").fetchdf()
            print("Data Table Schema:")
            print("=" * 60)
            print(f"{'Column Name':<30} {'Type':<15} {'Null':<10}")
            print("-" * 60)

            for _, row in schema_result.iterrows():
                column_name = row["column_name"]
                column_type = row["column_type"]
                null_allowed = row["null"]
                print(f"{column_name:<30} {column_type:<15} {null_allowed:<10}")

            print("-" * 60)
            print(f"Total {len(schema_result)} columns")
            print("=" * 60)

        except Exception as e:
            logger.error(f"Failed to get schema information: {e}")

    def _flatten_algo_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        algo_cols = [col for col in df.columns if col.startswith("algo_")]

        if not algo_cols:
            return df

        expr_list = []

        for col in df.columns:
            if not col.startswith("algo_"):
                expr_list.append(pl.col(col))

        algo_fields = ["top1", "top3", "top5", "avg3", "avg5", "mrr", "time"]
        for algo_col in algo_cols:
            for field_name in algo_fields:
                new_col_name = f"{algo_col}_{field_name}"
                expr_list.append(
                    pl.col(algo_col)
                    .map_elements(
                        lambda x, field=field_name: x.get(field, 0.0) if isinstance(x, dict) else 0.0,
                        return_dtype=pl.Float64,
                    )
                    .alias(new_col_name)
                )

        try:
            flattened_df = df.select(expr_list)
            return flattened_df
        except Exception as e:
            logger.error(f"Warning: Failed to flatten algo columns, excluding them: {e}")
            non_algo_cols = [col for col in df.columns if not col.startswith("algo_")]
            return df.select(non_algo_cols)

    def custom_sql(self, sql_query: str) -> pl.DataFrame:
        result_arrow = self.conn.execute(sql_query).arrow()
        result_df = pl.from_arrow(result_arrow)

        if not isinstance(result_df, pl.DataFrame):
            raise TypeError(f"Expected DataFrame, got {type(result_df)}")

        return result_df

    def _group_by_analysis(self, group_column_sql: str, group_column_name: str) -> pl.DataFrame:
        return self._multi_group_by_analysis([group_column_sql], [group_column_name])

    def _multi_group_by_analysis(self, group_columns_sql: list[str], group_column_names: list[str]) -> pl.DataFrame:
        if len(group_columns_sql) != len(group_column_names):
            raise ValueError("group_columns_sql and group_column_names must have the same length")

        algo_columns = self._get_algo_columns()

        algo_aggregations = []
        for col in algo_columns:
            if col.endswith(("_top1", "_top3", "_top5", "_mrr", "_time")):
                col_parts = col.replace("algo_", "").rsplit("_", 1)
                if len(col_parts) == 2:
                    algo_name, metric_type = col_parts
                    alias = f"avg_{algo_name}_{metric_type}"
                    algo_aggregations.append(f"AVG({col}) as {alias}")

        algo_agg_sql = ",\n            ".join(algo_aggregations)

        select_columns = []
        for i, (sql_expr, col_name) in enumerate(zip(group_columns_sql, group_column_names)):
            select_columns.append(f"{sql_expr} as {col_name}")

        select_clause = ",\n            ".join(select_columns)
        group_clause = ",\n            ".join(group_column_names)

        base_sql = f"""
        SELECT 
            {select_clause},
            COUNT(*) as count"""

        if algo_agg_sql:
            sql = (
                base_sql
                + ",\n            "
                + algo_agg_sql
                + f"""
        FROM data 
        GROUP BY {group_clause}
        ORDER BY count DESC
        """
            )
        else:
            sql = (
                base_sql
                + f"""
        FROM data 
        GROUP BY {group_clause}
        ORDER BY count DESC
        """
            )

        raw_result = self.custom_sql(sql)

        return self._post_process_multi_analysis_results(raw_result, group_column_names)

    def _post_process_multi_analysis_results(
        self, raw_result: pl.DataFrame, group_column_names: list[str]
    ) -> pl.DataFrame:
        if raw_result.height == 0:
            return pl.DataFrame()

        algo_cols = [col for col in raw_result.columns if col.startswith("avg_")]

        if not algo_cols:
            return raw_result

        algorithms = set()
        metrics = set()

        for col in algo_cols:
            parts = col.replace("avg_", "").rsplit("_", 1)
            if len(parts) == 2:
                algo_name, metric_type = parts
                algorithms.add(algo_name)
                metrics.add(metric_type)

        algorithms = sorted(list(algorithms))
        metrics = sorted(list(metrics))

        result_rows = []

        for row in raw_result.iter_rows(named=True):
            count = row["count"]

            for algo in algorithms:
                algo_row = {"count": count, "algorithm": algo}

                for group_col in group_column_names:
                    algo_row[group_col] = row[group_col]

                for metric in metrics:
                    col_name = f"avg_{algo}_{metric}"
                    value = row.get(col_name, None)
                    algo_row[metric] = value

                result_rows.append(algo_row)

        if result_rows:
            result_df = pl.DataFrame(result_rows)

            sort_columns = group_column_names + ["algorithm"]
            result_df = result_df.sort(sort_columns)
            return result_df
        else:
            return pl.DataFrame()

    def _post_process_analysis_results(self, raw_result: pl.DataFrame, group_column_name: str) -> pl.DataFrame:
        if raw_result.height == 0:
            return pl.DataFrame()

        algo_cols = [col for col in raw_result.columns if col.startswith("avg_")]

        if not algo_cols:
            return raw_result

        algorithms = set()
        metrics = set()

        for col in algo_cols:
            parts = col.replace("avg_", "").rsplit("_", 1)
            if len(parts) == 2:
                algo_name, metric_type = parts
                algorithms.add(algo_name)
                metrics.add(metric_type)

        algorithms = sorted(list(algorithms))
        metrics = sorted(list(metrics))

        result_rows = []

        for row in raw_result.iter_rows(named=True):
            group_value = row[group_column_name]
            count = row["count"]

            for algo in algorithms:
                algo_row = {group_column_name: group_value, "count": count, "algorithm": algo}

                for metric in metrics:
                    col_name = f"avg_{algo}_{metric}"
                    value = row.get(col_name, None)
                    algo_row[metric] = value

                result_rows.append(algo_row)

        if result_rows:
            result_df = pl.DataFrame(result_rows)
            result_df = result_df.sort([group_column_name, "algorithm"])
            return result_df
        else:
            return pl.DataFrame()

    def fault_category_analysis(self) -> pl.DataFrame:
        return self._group_by_analysis("fault_category", "fault_category")

    def fault_type_analysis(self) -> pl.DataFrame:
        return self._group_by_analysis("fault_type", "fault_type")

    def sdd_analysis(self) -> pl.DataFrame:
        group_sql = """CASE 
                WHEN "SDD@1" = 0 THEN 'SDD@1 = 0'
                ELSE 'SDD@1 > 0'
            END"""
        return self._group_by_analysis(group_sql, "sdd_category")

    def fault_category_and_sdd_analysis(self) -> pl.DataFrame:
        group_sql_list = [
            "fault_category",
            """CASE 
                WHEN "SDD@1" = 0 THEN 'SDD@1 = 0'
                ELSE 'SDD@1 > 0'
            END""",
        ]
        group_names = ["fault_category", "sdd_category"]
        return self._multi_group_by_analysis(group_sql_list, group_names)

    def fault_type_and_sdd_analysis(self) -> pl.DataFrame:
        group_sql_list = [
            "fault_type",
            """CASE 
                WHEN "SDD@1" = 0 THEN 'SDD@1 = 0'
                ELSE 'SDD@1 > 0'
            END""",
        ]
        group_names = ["fault_type", "sdd_category"]
        return self._multi_group_by_analysis(group_sql_list, group_names)

    def _get_algo_columns(self) -> list[str]:
        try:
            columns_result = self.conn.execute("PRAGMA table_info('data')").fetchdf()
            algo_columns = [row["name"] for _, row in columns_result.iterrows() if row["name"].startswith("algo_")]
            return algo_columns
        except Exception as e:
            logger.error(f"Failed to get algorithm columns: {e}")
            return []

    def close(self):
        self.conn.close()
