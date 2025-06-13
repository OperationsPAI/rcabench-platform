from ..config import get_config
from ..logging import timeit, logger
from ..utils.serde import save_parquet
from ..utils.env import debug

from collections import defaultdict

import polars as pl

PATTERN_REPLACEMENTS = [
    (
        r"(.*?)GET /api/v1/verifycode/verify/[0-9a-zA-Z]+",
        r"\1GET /api/v1/verifycode/verify/{verifyCode}",
    ),
    (
        r"(.*?)GET /api/v1/foodservice/foods/[0-9]{4}-[0-9]{2}-[0-9]{2}/[a-z]+/[a-z]+/[A-Z0-9]+",
        r"\1GET /api/v1/foodservice/foods/{date}/{startStation}/{endStation}/{tripId}",
    ),
    (
        r"(.*?)GET /api/v1/contactservice/contacts/account/[0-9a-f-]+",
        r"\1GET /api/v1/contactservice/contacts/account/{accountId}",
    ),
    (
        r"(.*?)GET /api/v1/userservice/users/id/[0-9a-f-]+",
        r"\1GET /api/v1/userservice/users/id/{userId}",
    ),
    (
        r"(.*?)GET /api/v1/consignservice/consigns/order/[0-9a-f-]+",
        r"\1GET /api/v1/consignservice/consigns/order/{id}",
    ),
    (
        r"(.*?)GET /api/v1/consignservice/consigns/account/[0-9a-f-]+",
        r"\1GET /api/v1/consignservice/consigns/account/{id}",
    ),
    (
        r"(.*?)GET /api/v1/executeservice/execute/collected/[0-9a-f-]+",
        r"\1GET /api/v1/executeservice/execute/collected/{orderId}",
    ),
    (
        r"(.*?)GET /api/v1/cancelservice/cancel/[0-9a-f-]+/[0-9a-f-]+",
        r"\1GET /api/v1/cancelservice/cancel/{orderId}/{loginId}",
    ),
    (
        r"(.*?)GET /api/v1/cancelservice/cancel/refound/[0-9a-f-]+",
        r"\1GET /api/v1/cancelservice/cancel/refound/{orderId}",
    ),
    (
        r"(.*?)GET /api/v1/executeservice/execute/execute/[0-9a-f-]+",
        r"\1GET /api/v1/executeservice/execute/execute/{orderId}",
    ),
]


PATTERN_REPLACEMENTS_POLARS = [(pat, rep.replace(r"\1", "${1}")) for pat, rep in PATTERN_REPLACEMENTS]


def _normalize_op_name(op_name: pl.Expr) -> pl.Expr:
    for pattern, replacement in PATTERN_REPLACEMENTS_POLARS:
        op_name = op_name.str.replace(pattern, replacement)
    return op_name


def tt_add_op_name(traces: pl.LazyFrame) -> pl.LazyFrame:
    lf = traces

    op_name = pl.concat_str(pl.col("service_name"), pl.col("span_name"), separator=" ")
    lf = lf.with_columns(_normalize_op_name(op_name).alias("op_name"))
    lf = lf.drop("span_name")

    return lf


@timeit(log_args=False)
def tt_fix_client_spans(traces: pl.DataFrame):
    id2op: dict[str, str] = {}
    id2parent: dict[str, str] = {}
    parent_child_map: defaultdict[str, set[str]] = defaultdict(set)

    selected = traces.select("span_id", "parent_span_id", "op_name")
    for span_id, parent_span_id, op_name in selected.iter_rows():
        assert isinstance(span_id, str) and span_id
        prev_op_name = id2op.get(span_id)
        if prev_op_name is not None:
            assert prev_op_name == op_name, (
                f"Duplicated span_id {span_id} with different op_name: `{prev_op_name}` vs `{op_name}`"
            )
        id2op[span_id] = op_name
        if parent_span_id:
            assert isinstance(parent_span_id, str)
            id2parent[span_id] = parent_span_id
            parent_child_map[parent_span_id].add(span_id)

    to_delete = set()
    fix_client_spans: dict[str, str] = {}
    for span_id, op_name in id2op.items():
        if op_name.endswith("GET") or op_name.endswith("POST"):
            children = parent_child_map[span_id]
            if len(children) == 0:
                to_delete.add(span_id)
            elif len(children) == 1:
                child = children.pop()
                child_op_name = id2op[child]
                real_op_name = op_name + " " + child_op_name.split(" ")[2]
                fix_client_spans[span_id] = real_op_name
            else:
                pass

    for span_id in to_delete:
        del id2op[span_id]
    id2op.update(fix_client_spans)
    del parent_child_map
    del to_delete

    if len(fix_client_spans) > 0:
        client_spans_df = pl.DataFrame(
            [{"span_id": span_id, "op_name": op_name} for span_id, op_name in fix_client_spans.items()]
        )
        del fix_client_spans

        if debug():
            save_parquet(client_spans_df, path=get_config().temp / "sdg" / "client_spans.parquet")

        traces = traces.join(client_spans_df, on="span_id", how="left")
        traces = traces.with_columns(pl.coalesce("op_name_right", "op_name").alias("op_name"))

    return traces, id2op, id2parent
