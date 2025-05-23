from ..defintion import SDG, DepEdge, DepKind, Indicator, PlaceKind, PlaceNode

from ....utils.serde import save_parquet
from ....spec.data import DATA_ROOT, TEMP_SDG
from ....logging import logger, timeit
from ....utils.env import debug

from collections import defaultdict
from pathlib import Path
import datetime

import polars as pl


@timeit()
def build_sdg_from_rcaeval(dataset: str, datapack: str, input_folder: Path) -> SDG:
    sdg = SDG()
    sdg.data["dataset"] = dataset
    sdg.data["datapack"] = datapack

    inject_time = load_inject_time(input_folder)
    sdg.data["inject_time"] = inject_time

    traces = load_traces_parquet(input_folder, inject_time)
    metrics = load_metrics_parquet(input_folder, inject_time)

    logger.debug("loading all dataframes")
    (traces, metrics) = pl.collect_all([traces, metrics])

    logger.debug(f"len(traces)={len(traces)}")
    logger.debug(f"len(metrics)={len(metrics)}")

    apply_traces(sdg, traces)
    del traces

    apply_metrics(sdg, metrics)
    del metrics

    # TODO: load more information

    return sdg


def load_inject_time(input_folder: Path) -> datetime.datetime:
    inject_time_file = input_folder / "inject_time.txt"

    with open(inject_time_file) as f:
        timestamp = int(f.read().strip())
    inject_time = datetime.datetime.fromtimestamp(timestamp, tz=datetime.UTC)

    logger.debug(f"inject_time=`{inject_time}`")
    return inject_time


def load_traces_parquet(input_folder: Path, inject_time: datetime.datetime) -> pl.LazyFrame:
    lf = pl.scan_parquet(input_folder / "traces.parquet")

    lf = lf.select(
        "time",
        "trace_id",
        "span_id",
        "service_name",
        "span_name",
        "duration",
        "parent_span_id",
    )

    lf = lf.with_columns((pl.col("time") >= inject_time).cast(pl.UInt8).alias("anomal"))

    lf = lf.with_columns(pl.col("duration").cast(pl.Float64))

    # add column `op_name`
    op_name = pl.concat_str(pl.col("service_name"), pl.col("span_name"), separator=" ")
    lf = lf.with_columns(op_name.alias("op_name")).drop("span_name")

    return lf


def apply_traces(sdg: SDG, traces: pl.DataFrame) -> None:
    id2op: dict[str, str] = {}
    id2parent: dict[str, str] = {}
    parent_child_map: defaultdict[str, set[str]] = defaultdict(set)

    selected = traces.select("span_id", "parent_span_id", "op_name")
    for i, (span_id, parent_span_id, op_name) in enumerate(selected.iter_rows()):
        assert isinstance(span_id, str) and span_id
        id2op[span_id] = op_name
        if parent_span_id:
            assert isinstance(parent_span_id, str)
            id2parent[span_id] = parent_span_id
            parent_child_map[parent_span_id].add(span_id)

    fix_client_spans: dict[str, str] = {}
    for span_id, op_name in id2op.items():
        if op_name.endswith("GET") or op_name.endswith("POST"):
            children = parent_child_map[span_id]
            if len(children) != 1:
                continue
            child = children.pop()
            child_op_name = id2op[child]
            real_op_name = op_name + " " + child_op_name.split(" ")[2]
            fix_client_spans[span_id] = real_op_name

    id2op.update(fix_client_spans)
    del parent_child_map

    if len(fix_client_spans) > 0:
        client_spans_df = pl.DataFrame(
            [
                {
                    "span_id": span_id,
                    "op_name": op_name,
                }
                for span_id, op_name in fix_client_spans.items()
            ]
        )
        del fix_client_spans

        if debug():
            save_parquet(client_spans_df, path=TEMP_SDG / "client_spans.parquet")

        traces = traces.join(client_spans_df, on="span_id", how="left")
        traces = traces.with_columns(pl.coalesce("op_name_right", "op_name").alias("op_name"))

    df_map = traces.partition_by("op_name", as_dict=True)
    for (op_name,), df in df_map.items():
        assert isinstance(op_name, str) and op_name
        assert len(df) > 0

        function_node = sdg.add_node(
            PlaceNode(kind=PlaceKind.function, self_name=op_name),
            strict=False,
        )

        function_node.add_indicator(
            Indicator(
                name="duration",
                df=df.select(
                    "time",
                    pl.col("duration").alias("value"),
                    "anomal",
                    "trace_id",
                    "span_id",
                    "parent_span_id",
                ),
            ),
            strict=False,
        )

        selected = df.select("span_id", "service_name")
        for span_id, service_name in selected.iter_rows():
            assert isinstance(span_id, str) and span_id
            assert isinstance(service_name, str) and service_name

            # function-function edges
            parent_span_id = id2parent.get(span_id)
            if parent_span_id:
                parent_op_name = id2op.get(parent_span_id)
                if parent_op_name:
                    parent_node = sdg.add_node(
                        PlaceNode(kind=PlaceKind.function, self_name=parent_op_name),
                        strict=False,
                    )
                    sdg.add_edge(
                        DepEdge(
                            src_id=parent_node.id,
                            dst_id=function_node.id,
                            kind=DepKind.calls,
                        ),
                        strict=False,
                    )

            assert service_name

            service_node = sdg.add_node(
                PlaceNode(kind=PlaceKind.service, self_name=service_name),
                strict=False,
            )

            sdg.add_edge(
                DepEdge(
                    src_id=service_node.id,
                    dst_id=function_node.id,
                    kind=DepKind.includes,
                ),
                strict=False,
            )

    top_op_names = set()
    for span_id, op_name in id2op.items():
        parent = id2parent.get(span_id)
        if parent is None or id2op.get(parent) is None:
            top_op_names.add(op_name)

    sdg.data["top_op_names"] = top_op_names

    if len(top_op_names) > 0:
        for op_name in top_op_names:
            logger.debug(f"top_op_name: {op_name}")
    else:
        logger.debug("No top_op_names")


def load_metrics_parquet(input_folder: Path, inject_time: datetime.datetime) -> pl.LazyFrame:
    lf = pl.scan_parquet(input_folder / "simple_metrics.parquet")

    lf = lf.with_columns((pl.col("time") >= inject_time).cast(pl.UInt8).alias("anomal"))

    return lf


def apply_metrics(sdg: SDG, metrics: pl.DataFrame) -> None:
    df_map = metrics.partition_by("metric", "attr.service_name", as_dict=True)
    del metrics

    for (metric, service_name), df in df_map.items():
        assert isinstance(metric, str) and metric
        assert isinstance(service_name, str) and service_name

        if is_constant_metric(df):
            logger.debug(f"ignore constant metric `{metric}`")
            continue

        service_node = sdg.add_node(
            PlaceNode(kind=PlaceKind.service, self_name=service_name),
            strict=False,
        )

        df = (
            df.lazy()
            .select(
                "time",
                pl.col("value").cast(pl.Float64),
                pl.col("anomal"),
            )
            .filter(
                pl.col("value").is_not_null(),
                pl.col("value").is_not_nan(),
            )
            .collect()
        )

        if len(df) == 0:
            continue

        service_node.add_indicator(Indicator(name=metric, df=df))


def is_constant_metric(df: pl.DataFrame) -> bool:
    col = pl.col("value")
    df = df.select(min=col.min(), max=col.max())
    min_value, max_value = df.row(0)
    assert isinstance(min_value, float)
    assert isinstance(max_value, float)
    return (max_value - min_value) < 1e-8
