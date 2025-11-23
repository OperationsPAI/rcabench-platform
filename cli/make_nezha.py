#!/usr/bin/env -S uv run -s

from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.sources.convert import convert_dataset, DatasetLoader, DatapackLoader, Label
from datetime import datetime, timedelta, timezone, UTC
from typing import Any, Optional
from pathlib import Path
import polars as pl
import json
import re
import os
import typer


def to_utc_time(time_str: str) -> datetime:
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=UTC)


def parse_time_utc(time_str: str) -> datetime | None:
    pattern = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
    match = pattern.search(time_str)
    if match:
        return to_utc_time(match.group())
    return None


class NezhaDatapackLoader(DatapackLoader):
    def __init__(self, case_data: dict[str, Any], source_dir: Path, inject_time: datetime, end_time: datetime):
        self.case_data = case_data
        self.source_dir = source_dir
        self.inject_time = inject_time
        self.end_time = end_time
        self.case_id = f"{self.inject_time.strftime('%Y-%m-%d_%H-%M')}_{self.case_data['fault_type']}"

    def name(self) -> str:
        return self.case_id

    def labels(self) -> list[Label]:
        return [
            Label(level="pod", name=self.case_data.get("injection_name", "unknown")),
            Label(level="type", name=self.case_data.get("fault_type", "unknown")),
        ]

    def data(self) -> dict[str, Any]:
        data = {}

        metric_dir = self.source_dir / "metric"
        if metric_dir.exists():
            metric_dfs = self._process_metrics(metric_dir)
            if metric_dfs:
                try:
                    data["metric.parquet"] = pl.concat(metric_dfs)
                except Exception as e:
                    logger.warning("Failed to concat metric data \nError: {}", e)

        trace_dir = self.source_dir / "trace"
        if trace_dir.exists():
            trace_dfs = self._process_traces(trace_dir)
            if trace_dfs:
                data["trace.parquet"] = pl.concat(trace_dfs)

        log_dir = self.source_dir / "log"
        if log_dir.exists():
            log_dfs = self._process_logs(log_dir)
            if log_dfs:
                data["log.parquet"] = pl.concat(log_dfs)

        data["fault_info.json"] = self.case_data
        return data

    def _process_metrics(self, metric_dir: Path) -> list[pl.DataFrame]:
        metric_dfs = []
        for metric_file in metric_dir.glob("*.csv"):
            if "metric" in metric_file.name:
                try:
                    df = pl.read_csv(
                        metric_file, schema_overrides={"SyscallRead": pl.Float64, "SyscallWrite": pl.Float64}
                    )

                    exclude_columns = ["Time", "TimeStamp", "PodName"]
                    metric_columns = [col for col in df.columns if col not in exclude_columns]

                    df_long = df.unpivot(
                        index=["Time", "PodName"], on=metric_columns, variable_name="metric", value_name="value"
                    )

                    df_long = df_long.rename({"Time": "time", "PodName": "service_name"})
                    df_long = df_long.with_columns(pl.col("value").cast(pl.Float64, strict=False))

                    df_long = df_long.with_columns(
                        pl.col("time")
                        .str.extract(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6})")
                        .str.strptime(pl.Datetime(time_zone="UTC"), "%Y-%m-%d %H:%M:%S%.f")
                        .alias("time")
                    )

                    filtered_df = df_long.filter(
                        (pl.col("time") >= self.inject_time) & (pl.col("time") <= self.end_time)
                    )

                    metric_dfs.append(filtered_df)
                except Exception as e:
                    logger.warning("Failed to load metric data {} \nError: {}", metric_file, e)
        return metric_dfs

    def _process_traces(self, trace_dir: Path) -> list[pl.DataFrame]:
        trace_dfs = []
        for trace_file in trace_dir.glob("*.csv"):
            try:
                parts = trace_file.name.split("_")
                if len(parts) < 2:
                    continue

                hour = int(parts[0])
                minute = int(parts[1])

                assert self.inject_time is not None, "注入时间解析失败"
                file_time = self.inject_time.replace(hour=hour, minute=minute, second=0, microsecond=0)

                if not (self.inject_time <= file_time <= self.end_time):
                    continue

                df = pl.read_csv(trace_file)

                # 处理ParentID=root
                df = df.with_columns(pl.col("ParentID").str.replace("root", ""))

                column_mapping = {
                    "trace_id": "TraceID",
                    "span_id": "SpanID",
                    "parent_span_id": "ParentID",
                    "span_name": "OperationName",
                    "duration": "Duration",
                    "service_name": "PodName",
                }

                for new_col, old_col in column_mapping.items():
                    if old_col in df.columns:
                        df = df.rename({old_col: new_col})

                df = df.with_columns(
                    pl.col("StartTimeUnixNano").cast(pl.Datetime(time_unit="ns", time_zone="UTC")).alias("time")
                )

                if "duration" in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col("duration") < 0)
                        .then(0)
                        .otherwise(pl.col("duration").cast(pl.Float64).round())
                        .cast(pl.UInt64)
                        .alias("duration")
                    )

                required_columns = list(column_mapping.keys()) + ["time"]
                for col in df.columns:
                    if col not in required_columns:
                        df = df.with_columns(pl.col(col).alias(f"attr.{col}"))

                selected_columns = required_columns
                for col in df.columns:
                    if col not in required_columns and col.startswith("attr."):
                        selected_columns.append(col)

                df = df.select(selected_columns)
                trace_dfs.append(df)
            except Exception as e:
                logger.warning("Failed to load trace data {} \nError: {}", trace_file, e)
        return trace_dfs

    def _process_logs(self, log_dir: Path) -> list[pl.DataFrame]:
        log_dfs = []
        for log_file in log_dir.glob("*.csv"):
            try:
                parts = log_file.name.split("_")
                if len(parts) < 2:
                    continue

                hour = int(parts[0])
                minute = int(parts[1])

                assert self.inject_time is not None
                file_time = self.inject_time.replace(hour=hour, minute=minute, second=0, microsecond=0)

                if not (self.inject_time <= file_time <= self.end_time):
                    continue

                df = pl.read_csv(log_file)

                column_mapping = {
                    "trace_id": "TraceID",
                    "span_id": "SpanID",
                    "service_name": "PodName",
                }

                for new_col, old_col in column_mapping.items():
                    if old_col in df.columns:
                        df = df.rename({old_col: new_col})

                if "Timestamp" in df.columns:
                    df = df.with_columns(
                        [
                            pl.col("Timestamp")
                            .str.extract(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})", 1)
                            .alias("time")
                        ]
                    )
                    df = df.with_columns(
                        [pl.col("time").str.strptime(pl.Datetime(time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f")]
                    )

                if "Log" in df.columns:
                    output_type = pl.Struct([pl.Field("message", pl.Utf8), pl.Field("level", pl.Utf8)])
                    df = df.with_columns(
                        pl.col("Log").map_elements(self._parse_log, return_dtype=output_type).struct.unnest()
                    )

                required_columns = list(column_mapping.keys()) + ["time", "level", "message"]
                selected_columns = ["time", "trace_id", "span_id", "service_name", "level", "message"]

                for col in df.columns:
                    if col not in required_columns:
                        df = df.with_columns(pl.col(col).alias(f"attr.{col}"))

                for col in df.columns:
                    if col not in required_columns and col.startswith("attr."):
                        selected_columns.append(col)

                df = df.select(selected_columns)
                log_dfs.append(df)
            except Exception as e:
                logger.warning("Failed to load trace data {} \nError: {}", log_file, e)
        return log_dfs

    def _parse_log(self, log_json_str: str) -> dict[str, str]:
        try:
            outer = json.loads(log_json_str)
            log_text = outer.get("log", "")

            try:
                inner = json.loads(log_text)
                msg = inner.get("message", "")
                severity = inner.get("severity", "")
                if msg and severity:
                    return {"message": msg, "level": severity.upper()}
            except ValueError:
                pass

            level_match = re.search(r"\b(INFO|WARNING|ERROR|CRITICAL|DEBUG)\b", log_text)
            level = level_match.group(1) if level_match else "INFO"

            msg_match = re.search(rf"\b{level}\s*-\s*(.*)", log_text)
            if msg_match:
                msg = msg_match.group(1).strip()
            else:
                cleaned_text = re.sub(r"^\d{2}:\d{2}:\d{2}\.\d+\s+", "", log_text)
                cleaned_text = re.sub(rf"\b{level}\b", "", cleaned_text).strip()
                msg = cleaned_text if cleaned_text else "not parsed"

            return {"message": msg, "level": level.upper()}
        except Exception:
            return {"message": "not parsed", "level": "INFO"}


class NezhaDatasetLoader(DatasetLoader):
    def __init__(self, source_dir: str, date: str):
        self.source_dir = Path(os.path.join(source_dir, date))
        self.date = date
        self.fault_list = self._load_fault_list()

    def _load_fault_list(self) -> list[dict[str, Any]]:
        fault_list_path = self.source_dir / f"{self.date}-fault_list.json"
        if not fault_list_path.exists():
            raise FileNotFoundError(f"injection not found: {fault_list_path}")
        with open(fault_list_path, encoding="utf-8") as f:
            fault_list = json.load(f)
        return [fault for hour in fault_list.values() for fault in hour]

    def name(self) -> str:
        return "nezha"

    def __len__(self) -> int:
        return len(self.fault_list)

    def __getitem__(self, index: int) -> NezhaDatapackLoader:
        case_data = self.fault_list[index]
        new_case_data = {}
        inject_time = parse_time_utc(case_data["inject_time"])
        assert inject_time is not None
        if index < len(self.fault_list) - 1:
            next_case = self.fault_list[index + 1]
            next_inject_time = parse_time_utc(next_case["inject_time"])
            assert next_inject_time is not None
            end_time = next_inject_time - timedelta(minutes=1)
        else:
            end_time = inject_time + timedelta(minutes=10)

        service_name = case_data["inject_pod"].split("service")[0] + "service"
        new_case_data = {
            "injection_name": service_name,
            "fault_type": case_data["inject_type"],
            "fault_start_time": inject_time,
            "fault_end_time": end_time,
            "normal_start_time": "",
            "normal_end_time": "",
        }
        return NezhaDatapackLoader(new_case_data, self.source_dir, inject_time, end_time)


class MultiDateDatasetLoader(DatasetLoader):
    def __init__(self, source_dir: str, dates: list[str], name: str):
        self.source_dir = source_dir
        self.dates = dates
        self._name = name
        self.all_cases = []
        for date in dates:
            loader = NezhaDatasetLoader(source_dir, date)
            for i in range(len(loader)):
                self.all_cases.append(loader[i])

    def name(self) -> str:
        return self._name

    def __len__(self) -> int:
        return len(self.all_cases)

    def __getitem__(self, index: int) -> NezhaDatapackLoader:
        return self.all_cases[index]


@app.command(help="Convert Nezha dataset to RCABench format")
@timeit()
def run(
    source_dir: str = "data/nezha/rca_data",
    ob_dates: list[str] = typer.Option([], help="ob系统的所有日期,2022-08-22 2022-08-23"),
    tt_dates: list[str] = typer.Option([], help="tt系统的所有日期,2023-01-29 2023-01-30"),
):
    if ob_dates:
        logger.info(f"开始处理nezha_ob: {ob_dates}")
        loader_ob = MultiDateDatasetLoader(source_dir, ob_dates, name="nezha_ob")
        convert_dataset(loader_ob, skip_finished=False, parallel=4)
        logger.info("nezha_ob数据集处理完成！")
    if tt_dates:
        logger.info(f"开始处理nezha_tt: {tt_dates}")
        loader_tt = MultiDateDatasetLoader(source_dir, tt_dates, name="nezha_tt")
        convert_dataset(loader_tt, skip_finished=False, parallel=4)
        logger.info("nezha_tt数据集处理完成！")


@app.command(help="Create a symbolic link from source path to target path for Eadro dataset access")
@timeit()
def create_link(
    source_path: str = typer.Option(
        "/mnt/jfs/Nezha/",
        "--source-path",
        "-s",
        help="Source path where the Nezha dataset is located (default: /mnt/jfs/Nezha/)",
    ),
    target_path: str = typer.Option(
        "data/nezha", "--target-path", "-t", help="Target path where the symbolic link will be created (default: data)"
    ),
):
    try:
        if not os.path.exists(source_path):
            logger.error(f"Source path does not exist: {source_path}")
            return

        if os.path.exists(target_path) or os.path.islink(target_path):
            if os.path.islink(target_path):
                os.unlink(target_path)
                logger.info(f"Removed existing symbolic link: {target_path}")
            else:
                logger.error(
                    f"Target path {target_path} already exists and is not a symbolic link. Please handle manually."
                )
                return

        os.symlink(source_path, target_path)
        logger.info(f"Successfully created symbolic link: {target_path} -> {source_path}")

    except OSError as e:
        logger.error(f"Failed to create symbolic link: {e}")


if __name__ == "__main__":
    app()
