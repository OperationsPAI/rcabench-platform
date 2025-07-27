#!/usr/bin/env -S uv run -s

import datetime
from pathlib import Path
from typing import Any

import polars as pl

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.sources.convert import DatapackLoader, DatasetLoader, Label

"""single path overview
    nn@debian ~/w/r/d/a/a/A/aiops2021-2> pwd
    /home/nn/workspace/rcabench-platform/data/aiops/aiops2021-main/AIOps2021/aiops2021-2
    nn@debian ~/w/r/d/a/a/A/aiops2021-2> ls
    0304/  0305/  0306/  0307/  0309/  0310/  0312/  0323/  0324/  0325/

    nn@debian ~/w/r/d/a/a/A/a/0304> pwd
/home/nn/workspace/rcabench-platform/data/aiops/aiops2021-main/AIOps2021/aiops2021-2/0304
nn@debian ~/w/r/d/a/a/A/a/0304> tree
.
├── logs
│   ├── log_apache_access_log_0304.csv
│   ├── log_catalina_0304.csv
│   ├── log_gc_0304.csv
│   ├── log_localhost_0304.csv
│   └── log_localhost_access_log_0304.csv
├── metric
│   ├── kpi_0304.csv
│   └── metric_0304.csv
└── trace
    └── trace_0304.csv

4 directories, 8 files
"""


def convert_traces(src: Path) -> pl.LazyFrame:
    """Convert AIOps21 trace data to standard format.

    head trace_0304.csv
    timestamp,cmdb_id,parent_id,span_id,trace_id,duration
    1614787199628,dockerA2,369-bcou-dle-way1-c514cf30-43410@0824-2f0e47a816-17492,21030300016145905763,gw0120210304000517192504,19
    1614787199635,dockerA2,21030300016145905763,21030300016145905768,gw0120210304000517192504,1
    """
    assert src.exists(), f"Source file does not exist: {src}"

    lf = pl.scan_csv(src, infer_schema_length=50000)

    # Convert to standard format
    lf = lf.select(
        # Convert timestamp from milliseconds to datetime UTC
        pl.from_epoch("timestamp", time_unit="ms").dt.replace_time_zone("UTC").alias("time"),
        pl.col("trace_id").cast(pl.String).alias("trace_id"),
        pl.col("span_id").cast(pl.String).alias("span_id"),
        pl.col("parent_id").cast(pl.String).alias("parent_span_id"),
        pl.col("cmdb_id").cast(pl.String).alias("service_name"),
        # Generate span_name from service_name for now, as we don't have operation names
        pl.col("cmdb_id").cast(pl.String).alias("span_name"),
        # Convert duration from milliseconds to nanoseconds
        pl.col("duration").cast(pl.UInt64).mul(1_000_000).alias("duration"),
    )

    # Fill empty parent_span_id with empty string (root spans)
    lf = lf.with_columns(pl.col("parent_span_id").fill_null(""))

    # Set parent_span_id to empty if it equals span_id (self-referencing spans)
    lf = lf.with_columns(
        pl.when(pl.col("span_id") == pl.col("parent_span_id"))
        .then(pl.lit(""))
        .otherwise(pl.col("parent_span_id"))
        .alias("parent_span_id")
    )

    lf = lf.sort("time")

    return lf


def convert_metrics(src: Path) -> pl.LazyFrame:
    """Convert AIOps21 metric data to standard format.

    head metric_0304.csv
    timestamp,cmdb_id,kpi_name,value
    1614787200,Tomcat04,OSLinux-CPU_CPU_CPUCpuUtil,26.2957
    1614787200,Mysql02,Mysql-MySQL_3306_Innodb data pending writes,0.0
    1614787200,Mysql02,Mysql-MySQL_3306_Innodb data pending reads,0.0
    """
    assert src.exists(), f"Source file does not exist: {src}"

    lf = pl.scan_csv(src, infer_schema_length=50000)

    # Convert to standard format
    lf = lf.select(
        # Convert timestamp from seconds to datetime UTC
        pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC").alias("time"),
        pl.col("kpi_name").cast(pl.String).alias("metric"),
        pl.col("value").cast(pl.Float64).alias("value"),
        pl.col("cmdb_id").cast(pl.String).alias("service_name"),
    )

    lf = lf.sort("time")

    return lf


def convert_logs(src_folder: Path) -> pl.LazyFrame:
    """Convert AIOps21 log data to standard format.

    head log_apache_access_log_0304.csv
    log_id,timestamp,cmdb_id,log_name,value
    ac189145b045a6f0228c873b7476066a,1614831458,apache02,apache_access_log,"IPAddress ..."
    c44044aec240d5cf69e65f0cd8935ee5,1614831459,apache02,apache_access_log,"IPAddress ..."
    """
    # AIOps21 has multiple log files in logs/ directory
    log_files = list((src_folder / "logs").glob("*.csv"))

    if not log_files:
        # Return empty dataframe with correct schema if no log files found
        return pl.LazyFrame(
            schema={
                "time": pl.Datetime,
                "trace_id": pl.String,
                "span_id": pl.String,
                "service_name": pl.String,
                "level": pl.String,
                "message": pl.String,
            }
        )

    dfs = []
    for log_file in log_files:
        lf = pl.scan_csv(log_file, infer_schema_length=50000)

        # Convert to standard format
        lf = lf.select(
            # Convert timestamp from seconds to datetime UTC
            pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC").alias("time"),
            pl.lit("").alias("trace_id"),  # AIOps21 logs don't have trace_id
            pl.lit("").alias("span_id"),  # AIOps21 logs don't have span_id
            pl.col("cmdb_id").cast(pl.String).alias("service_name"),
            pl.lit("INFO").alias("level"),  # Default level since not provided
            pl.col("value").cast(pl.String).alias("message"),
        )

        dfs.append(lf)

    # Concatenate all log files
    combined_lf = pl.concat(dfs)
    combined_lf = combined_lf.sort("time")

    return combined_lf


class AIops21DatapackLoader(DatapackLoader):
    def __init__(
        self,
        src_folder: Path,
        dataset: str,
        datapack: str,
        fault_case: dict,
        date_str: str,
    ) -> None:
        self._src_folder = src_folder
        self._dataset = dataset
        self._datapack = datapack
        self._fault_case = fault_case
        self._date_str = date_str

    def name(self) -> str:
        return self._datapack

    def labels(self) -> list[Label]:
        return [Label(level="service", name=self._fault_case["service"])]

    def _get_data_time_range(self) -> tuple[pl.Datetime, pl.Datetime]:
        """Get the full time range of available data for the day."""
        # Check traces file first
        trace_file = self._src_folder / "trace" / f"trace_{self._date_str}.csv"
        if trace_file.exists():
            trace_lf = convert_traces(trace_file)
            trace_times = trace_lf.select(
                [
                    pl.col("time").min().alias("min_time"),
                    pl.col("time").max().alias("max_time"),
                ]
            ).collect()
            min_time = trace_times["min_time"][0]
            max_time = trace_times["max_time"][0]
            return min_time, max_time

        # Fallback to metrics file
        metric_file = self._src_folder / "metric" / f"metric_{self._date_str}.csv"
        if metric_file.exists():
            metric_lf = convert_metrics(metric_file)
            metric_times = metric_lf.select(
                [
                    pl.col("time").min().alias("min_time"),
                    pl.col("time").max().alias("max_time"),
                ]
            ).collect()
            min_time = metric_times["min_time"][0]
            max_time = metric_times["max_time"][0]
            return min_time, max_time

        # If no data files available, use fault injection times as fallback
        return self._fault_case["start_time"], self._fault_case["end_time"]

    def _to_iso_string(self, time_obj) -> str:
        """Convert time object to ISO format string."""
        if hasattr(time_obj, "isoformat"):
            return time_obj.isoformat()
        else:
            # For Polars datetime objects, convert to string
            return str(time_obj)

    def _calculate_time_windows(self) -> tuple[Any, Any, Any, Any]:
        """Calculate normal and fault time windows.

        Returns:
            tuple: (normal_start_time, normal_end_time, fault_start_time, fault_end_time)
        """
        # Get fault injection time window
        fault_start_time = self._fault_case["start_time"]
        fault_end_time = self._fault_case["end_time"]

        # Get full data time range for the day
        data_start_time, data_end_time = self._get_data_time_range()

        # Calculate normal time window: before fault injection with 15-minute limit
        normal_end_time = fault_start_time
        max_normal_duration = datetime.timedelta(minutes=15)
        earliest_normal_start = fault_start_time - max_normal_duration

        # Take the later time: either data starts or 15 minutes before fault
        # This implements: min(15 minutes, time available before fault)
        if data_start_time > earliest_normal_start:
            normal_start_time = data_start_time
        else:
            normal_start_time = earliest_normal_start

        return normal_start_time, normal_end_time, fault_start_time, fault_end_time

    def data(self) -> dict[str, Any]:
        # Calculate time windows: [normal_start, normal_end, fault_start, fault_end]
        normal_start_time, normal_end_time, fault_start_time, fault_end_time = self._calculate_time_windows()

        # Create data dict with filtered data for the specific time periods
        # Data range covers from normal start to fault end
        overall_start_time = normal_start_time
        overall_end_time = fault_end_time

        data_dict: dict[str, Any] = {
            "traces.parquet": self._filter_traces_by_time(overall_start_time, overall_end_time),
            "metrics.parquet": self._filter_metrics_by_time(overall_start_time, overall_end_time),
            "logs.parquet": self._filter_logs_by_time(overall_start_time, overall_end_time),
        }

        # Add fault case metadata with both normal and fault time periods
        metadata = {
            "injection_name": self._fault_case["service"],
            "fault_type": self._fault_case["anomaly_type"],
            "fault_start_time": self._to_iso_string(fault_start_time),
            "fault_end_time": self._to_iso_string(fault_end_time),
            "normal_start_time": self._to_iso_string(normal_start_time),
            "normal_end_time": self._to_iso_string(normal_end_time),
            # Additional metadata for compatibility
            "fault_id": self._fault_case["id"],
            "fault_category": self._fault_case["fault_category"],
            "fault_content": self._fault_case["fault_content"],
            "fault_time": self._to_iso_string(self._fault_case["fault_time"]),
            "data_type": self._fault_case["data_type"],
        }
        data_dict["metadata.json"] = metadata

        return data_dict

    def _filter_traces_by_time(self, start_time, end_time) -> pl.LazyFrame:
        """Filter trace data by specified time window."""
        trace_file = self._src_folder / "trace" / f"trace_{self._date_str}.csv"
        if not trace_file.exists():
            # Return empty dataframe with correct schema
            return pl.LazyFrame(
                schema={
                    "time": pl.Datetime,
                    "trace_id": pl.String,
                    "span_id": pl.String,
                    "parent_span_id": pl.String,
                    "service_name": pl.String,
                    "span_name": pl.String,
                    "duration": pl.UInt64,
                }
            )

        lf = convert_traces(trace_file)

        # Filter by the specified time window (no buffer)
        return lf.filter((pl.col("time") >= start_time) & (pl.col("time") <= end_time))

    def _filter_metrics_by_time(self, start_time, end_time) -> pl.LazyFrame:
        """Filter metric data by specified time window."""
        metric_file = self._src_folder / "metric" / f"metric_{self._date_str}.csv"
        if not metric_file.exists():
            # Return empty dataframe with correct schema
            return pl.LazyFrame(
                schema={
                    "time": pl.Datetime,
                    "metric": pl.String,
                    "value": pl.Float64,
                    "service_name": pl.String,
                }
            )

        lf = convert_metrics(metric_file)

        # Filter by the specified time window (no buffer)
        return lf.filter((pl.col("time") >= start_time) & (pl.col("time") <= end_time))

    def _filter_logs_by_time(self, start_time, end_time) -> pl.LazyFrame:
        """Filter log data by specified time window."""
        lf = convert_logs(self._src_folder)

        # Filter by the specified time window (no buffer)
        return lf.filter((pl.col("time") >= start_time) & (pl.col("time") <= end_time))


class AIops21DatasetLoader(DatasetLoader):
    def __init__(self, src_folder: Path, dataset: str):
        self._src_folder = src_folder
        self._dataset = dataset

        datapack_loaders = []

        # Get fault cases grouped by date
        fault_cases_by_date = get_fault_cases_by_date()

        for date_str, fault_cases in fault_cases_by_date.items():
            date_path = src_folder / date_str

            if not date_path.exists() or not date_path.is_dir():
                logger.warning(f"Skipping {date_str}: date folder does not exist")
                continue

            # Check if required files exist
            trace_file = date_path / "trace" / f"trace_{date_str}.csv"
            metric_file = date_path / "metric" / f"metric_{date_str}.csv"

            if not (trace_file.exists() and metric_file.exists()):
                logger.warning(f"Skipping {date_str}: missing required files")
                continue

            # Create a datapack for each fault case in this date
            for i, fault_case in enumerate(fault_cases):
                datapack = f"aiops21_{date_str}_case_{i + 1:02d}_{fault_case['row_idx']:03d}"

                loader = AIops21DatapackLoader(
                    src_folder=date_path,
                    dataset=dataset,
                    datapack=datapack,
                    fault_case=fault_case,
                    date_str=date_str,
                )

                datapack_loaders.append(loader)

        self._datapack_loaders = datapack_loaders

    def name(self) -> str:
        return self._dataset

    def __len__(self) -> int:
        return len(self._datapack_loaders)

    def __getitem__(self, index: int) -> DatapackLoader:
        return self._datapack_loaders[index]


def load_groundtruth():
    """
    Load and process groundtruth data for fault injection times.

    CSV structure:
    ,id,service,anomaly_type,故障类别,故障内容,time,st_time,ed_time,data_type
    0,1479,apache01,MEMORY,资源故障,内存使用率过高,1614829800000,
        2021-03-04 11:50:00.000000,2021-03-04 11:55:00.000000,train
    """
    df = pl.read_csv("data/aiops/aiops2021-main/AIOps2021/aiops21_groundtruth.csv")

    # Convert time from milliseconds to datetime and sort by time
    df = df.with_columns(
        [
            pl.from_epoch("time", time_unit="ms").dt.replace_time_zone("UTC").alias("fault_time"),
            pl.col("st_time")
            .str.to_datetime("%Y-%m-%d %H:%M:%S%.f")
            .dt.replace_time_zone("Asia/Shanghai")
            .dt.convert_time_zone("UTC")
            .alias("start_time"),
            pl.col("ed_time")
            .str.to_datetime("%Y-%m-%d %H:%M:%S%.f")
            .dt.replace_time_zone("Asia/Shanghai")
            .dt.convert_time_zone("UTC")
            .alias("end_time"),
        ]
    ).sort("fault_time")

    return df


def get_fault_cases_by_date():
    """
    Group fault injection cases by date based on groundtruth data.
    Returns a dictionary mapping date strings to lists of fault cases.
    """
    gt_df = load_groundtruth()

    # Extract date from fault_time and group cases
    cases_by_date = {}

    for row_idx, row in enumerate(gt_df.iter_rows(named=True)):
        fault_time = row["fault_time"]
        date_str = fault_time.strftime("%m%d")  # Format as "0304", "0305", etc.

        case_info = {
            "id": row["id"],
            "row_idx": row_idx,  # Add unique row index to handle duplicate IDs
            "service": row["service"],
            "anomaly_type": row["anomaly_type"],
            "fault_category": row["故障类别"],
            "fault_content": row["故障内容"],
            "fault_time": fault_time,
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "data_type": row["data_type"],
        }

        if date_str not in cases_by_date:
            cases_by_date[date_str] = []
        cases_by_date[date_str].append(case_info)

    # Sort cases within each date by fault_time
    for date_str in cases_by_date:
        cases_by_date[date_str].sort(key=lambda x: x["fault_time"])

    return cases_by_date


@app.command()
def local_test():
    src = Path("data/aiops/aiops2021-main/AIOps2021/aiops2021-2/0306")
    # Test the conversion functions
    logger.info(f"Testing conversion functions with {src}")

    # Test traces conversion
    traces_file = src / "trace" / "trace_0306.csv"
    if traces_file.exists():
        traces_lf = convert_traces(traces_file)
        logger.info(f"Traces converted: {traces_lf.collect().shape}")

    # Test metrics conversion
    metrics_file = src / "metric" / "metric_0306.csv"
    if metrics_file.exists():
        metrics_lf = convert_metrics(metrics_file)
        logger.info(f"Metrics converted: {metrics_lf.collect().shape}")

    # Test logs conversion
    logs_lf = convert_logs(src)
    logger.info(f"Logs converted: {logs_lf.collect().shape}")


@app.command()
@timeit()
def run():
    """Convert AIOps21 dataset to RCABench format."""
    from rcabench_platform.v2.sources.convert import convert_dataset

    src_folder = Path("data/aiops/aiops2021-main/AIOps2021/aiops2021-2")
    dataset_name = "aiops21"

    if not src_folder.exists():
        logger.error(f"Source folder not found: {src_folder}")
        return

    loader = AIops21DatasetLoader(src_folder, dataset_name)
    logger.info(f"Found {len(loader)} datapacks in {src_folder}")

    convert_dataset(loader, parallel=4, skip_finished=True)
    logger.info(f"Conversion completed for dataset: {dataset_name}")


if __name__ == "__main__":
    app()
