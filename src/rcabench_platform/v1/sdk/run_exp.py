from ..cli.main import app, logger
from ..logging import timeit

from pprint import pprint
from pathlib import Path
from typing import Any
import importlib.util
import inspect
import sys
import os

import pandas as pd


@timeit()
def load_module(source: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    spec.loader.exec_module(module)

    return module


def build_params() -> dict[str, Any]:
    workspace = Path(os.environ.get("WORKSPACE", "/app"))
    input_path = Path(os.environ.get("INPUT_PATH", "/app/input"))
    output_path = Path(os.environ.get("OUTPUT_PATH", "/app/output"))

    assert workspace.is_dir()
    assert input_path.is_dir()
    assert output_path.is_dir()

    files = {
        "normal_log_file": input_path / "normal_logs.parquet",
        "normal_trace_file": input_path / "normal_traces.parquet",
        "normal_trace_id_ts_file": input_path / "normal_trace_id_ts.parquet",
        "normal_metric_file": input_path / "normal_metrics.parquet",
        "normal_metric_sum_file": input_path / "normal_metrics_sum.parquet",
        "normal_metric_summary_file": input_path / "normal_metrics_summary.parquet",
        "normal_metric_histogram_file": input_path / "normal_metrics_histogram.parquet",
        "normal_event_file": input_path / "normal_events.parquet",
        "normal_profiling_file": input_path / "normal_profilings.parquet",
        "abnormal_log_file": input_path / "abnormal_logs.parquet",
        "abnormal_trace_file": input_path / "abnormal_traces.parquet",
        "abnormal_trace_id_ts_file": input_path / "abnormal_trace_id_ts.parquet",
        "abnormal_metric_file": input_path / "abnormal_metrics.parquet",
        "abnormal_metric_sum_file": input_path / "abnormal_metrics_sum.parquet",
        "abnormal_metric_summary_file": input_path / "abnormal_metrics_summary.parquet",
        "abnormal_metric_histogram_file": input_path / "abnormal_metrics_histogram.parquet",
        "abnormal_event_file": input_path / "abnormal_events.parquet",
        "abnormal_profiling_file": input_path / "abnormal_profilings.parquet",
    }

    return dict(
        workspace=workspace,
        input_path=input_path,
        output_path=output_path,
        **files,
    )


def show_params(params: dict[str, Any]) -> None:
    pprint(params)


@app.command()
@timeit()
def run(entrypoint: Path | None = None):
    if entrypoint:
        assert entrypoint.suffix == ".py"
        module = load_module(entrypoint, module_name="rca")
        func = getattr(module, "start_rca")
        assert func is not None
        assert inspect.isfunction(func)
    else:
        func = show_params

    params = build_params()

    if inspect.iscoroutinefunction(func):
        import asyncio

        asyncio.run(func(params))
    else:
        func(params)


@timeit(log_args=False)
def submit_conclusion_csv(df: pd.DataFrame):
    output_path = os.environ["OUTPUT_PATH"]
    assert output_path is not None and isinstance(output_path, str)

    output_path = Path(output_path)
    assert output_path.is_dir()

    output_path = output_path / "conclusion.csv"
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    app()
