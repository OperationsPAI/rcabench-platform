import base64
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import streamlit as st


def load_json_file(file_path: Path) -> dict[str, Any]:
    """Load JSON file.

    Args:
        file_path: Path to the JSON file.

    Returns:
        Dictionary containing the JSON data.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Failed to load JSON file: {file_path}, error: {str(e)}")
        return {}


def load_parquet_file(file_path: Path) -> pl.DataFrame:
    """Load Parquet file.

    Args:
        file_path: Path to the Parquet file.

    Returns:
        DataFrame containing the Parquet data.
    """
    try:
        return pl.read_parquet(file_path)
    except Exception as e:
        st.error(f"Failed to load Parquet file: {file_path}, error: {str(e)}")
        return pl.DataFrame()


def validate_dataset_folder(folder_path: Path) -> tuple[bool, list[str]]:
    """Validate if dataset folder contains required files.

    Args:
        folder_path: Path to the dataset folder.

    Returns:
        Tuple containing validation result and list of missing files.
    """
    from rcabench_platform.v2.tools.label.config import REQUIRED_FILES

    missing_files = []
    if not folder_path.exists():
        return False, ["Folder does not exist"]

    for required_file in REQUIRED_FILES:
        file_path = folder_path / required_file
        if not file_path.exists():
            missing_files.append(required_file)

    return len(missing_files) == 0, missing_files


def format_timestamp(timestamp: int, timezone: str = "Asia/Shanghai") -> str:
    """Format timestamp to human-readable string.

    Args:
        timestamp: Unix timestamp.
        timezone: Timezone string (currently unused).

    Returns:
        Formatted timestamp string.
    """
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(timestamp)


def safe_get_column_names(df: pl.DataFrame, exclude_cols: list[str] | None = None) -> list[str]:
    """Safely get dataframe column names.

    Args:
        df: Input dataframe.
        exclude_cols: Columns to exclude from the result.

    Returns:
        List of column names.
    """
    if df.is_empty():
        return []

    exclude_cols = exclude_cols or []
    return [col for col in df.columns if col not in exclude_cols]


def filter_dataframe_by_time(
    df: pl.DataFrame, time_col: str, start_time: float | None = None, end_time: float | None = None
) -> pl.DataFrame:
    """Filter dataframe by time range using Polars for acceleration.

    Args:
        df: Input dataframe.
        time_col: Name of the time column.
        start_time: Start time filter.
        end_time: End time filter.

    Returns:
        Filtered dataframe.
    """
    if df.is_empty() or time_col not in df.columns:
        return df

    if start_time is None and end_time is None:
        return df

    try:
        # Use Polars for fast time filtering
        filtered_df = df

        if start_time is not None:
            filtered_df = filtered_df.filter(pl.col(time_col) >= start_time)

        if end_time is not None:
            filtered_df = filtered_df.filter(pl.col(time_col) <= end_time)

        return filtered_df

    except Exception as e:
        st.warning(f"Polars time filtering failed: {str(e)}")
        return df


def search_logs(df: pl.DataFrame, search_term: str, search_columns: list[str] | None = None) -> pl.DataFrame:
    """Search in log dataframe using Polars for acceleration.

    Args:
        df: Input dataframe.
        search_term: Term to search for.
        search_columns: Columns to search in.

    Returns:
        Filtered dataframe containing search results.
    """
    if df.is_empty() or not search_term.strip():
        return df

    if search_columns is None:
        search_columns = [col for col in df.columns if df[col].dtype == pl.String]

    try:
        # Use Polars for fast text search
        search_term_lower = search_term.lower()

        # Build search conditions for Polars
        search_conditions = []

        for col in search_columns:
            if col in df.columns:
                search_conditions.append(
                    pl.col(col).cast(pl.String).str.to_lowercase().str.contains(search_term_lower, literal=True)
                )

        if search_conditions:
            # Combine conditions with OR
            combined_condition = search_conditions[0]
            for condition in search_conditions[1:]:
                combined_condition = combined_condition | condition

            return df.filter(combined_condition)
        else:
            return df

    except Exception as e:
        st.warning(f"Polars search failed: {str(e)}")
        return df


def calculate_time_range_from_percentage(env_data: dict, start_pct: float, end_pct: float) -> tuple[float, float]:
    try:
        normal_start = float(env_data.get("NORMAL_START", 0))
        abnormal_end = float(env_data.get("ABNORMAL_END", 0))

        total_duration = abnormal_end - normal_start

        start_time = normal_start + (start_pct / 100) * total_duration
        end_time = normal_start + (end_pct / 100) * total_duration

        return start_time, end_time
    except Exception:
        return 0, 0


def get_injection_time_markers(env_data: dict) -> dict[str, datetime]:
    try:
        markers = {}
        for key, env_key in [
            ("normal_start", "NORMAL_START"),
            ("normal_end", "NORMAL_END"),
            ("abnormal_start", "ABNORMAL_START"),
            ("abnormal_end", "ABNORMAL_END"),
        ]:
            if env_key in env_data:
                timestamp = float(env_data[env_key])
                markers[key] = datetime.fromtimestamp(timestamp)
        return markers
    except Exception as e:
        print(f"Error parsing time markers: {e}")
        return {}


def format_bytes(bytes_value: int) -> str:
    """Format byte size"""
    value = float(bytes_value)
    for unit in ["B", "KB", "MB", "GB"]:
        if value < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TB"


def truncate_string(s: str, max_length: int = 100) -> str:
    if len(s) <= max_length:
        return s
    return s[: max_length - 3] + "..."


@st.cache_data(ttl=3600)
def cached_load_parquet(file_path: str) -> pl.DataFrame:
    """Cached Parquet file loader with Polars optimization."""
    df = load_parquet_file(Path(file_path))
    return df


@st.cache_data(ttl=3600)
def cached_load_json(file_path: str) -> dict[str, Any]:
    return load_json_file(Path(file_path))


def create_download_link(df: pl.DataFrame, filename: str, link_text: str) -> str:
    csv = df.write_csv()
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{link_text}</a>'
    return href


_data_loader = None
_label_manager = None


def get_data_loader():
    global _data_loader
    if _data_loader is None:
        from .data_loader import DataLoader

        _data_loader = DataLoader()
    return _data_loader


def get_label_manager():
    global _label_manager
    if _label_manager is None:
        from .label_manager import LabelManager

        _label_manager = LabelManager()
    return _label_manager


def query_dataframe_with_polars(df: pl.DataFrame, query: str) -> pl.DataFrame:
    """Execute a Polars query on a DataFrame.

    Args:
        df: Input DataFrame
        query: Polars query expression to execute

    Returns:
        Result DataFrame
    """
    if df.is_empty():
        return df

    try:
        # This is a placeholder - Polars doesn't support SQL queries directly
        # Instead, we would use Polars expressions
        st.warning("query_dataframe_with_polars: SQL queries not directly supported in Polars")
        return df
    except Exception as e:
        st.warning(f"Polars query failed: {str(e)}, returning original DataFrame")
        return df


def aggregate_dataframe_with_polars(df: pl.DataFrame, group_by: list[str], agg_funcs: dict[str, str]) -> pl.DataFrame:
    """Perform aggregation using Polars for better performance.

    Args:
        df: Input DataFrame
        group_by: Columns to group by
        agg_funcs: Dictionary mapping column names to aggregation functions

    Returns:
        Aggregated DataFrame
    """
    if df.is_empty():
        return df

    try:
        # Build aggregation expressions for Polars
        agg_exprs = []
        for col, func in agg_funcs.items():
            if func == "mean":
                agg_exprs.append(pl.col(col).mean().alias(f"{col}_{func}"))
            elif func == "sum":
                agg_exprs.append(pl.col(col).sum().alias(f"{col}_{func}"))
            elif func == "count":
                agg_exprs.append(pl.col(col).count().alias(f"{col}_{func}"))
            elif func == "min":
                agg_exprs.append(pl.col(col).min().alias(f"{col}_{func}"))
            elif func == "max":
                agg_exprs.append(pl.col(col).max().alias(f"{col}_{func}"))
            else:
                st.warning(f"Unsupported aggregation function: {func}")

        result_df = df.group_by(group_by).agg(agg_exprs)
        return result_df
    except Exception as e:
        st.warning(f"Polars aggregation failed: {str(e)}")
        return df
