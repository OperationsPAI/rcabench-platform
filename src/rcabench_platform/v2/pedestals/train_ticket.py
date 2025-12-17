import re
from collections import defaultdict

import polars as pl

from ..config import get_config
from ..logging import timeit
from ..utils.env import debug
from ..utils.serde import save_parquet
from .registry import Pedestal, register_pedestal

__all__ = [
    "TrainTicketPedestal",
]


@register_pedestal("ts")
class TrainTicketPedestal(Pedestal):
    """Processes Train-Ticket microservice trace data."""

    _BLACK_LIST = [
        "admin",
        "voucher",
        "avatar",
        "ts-gateway-service",
        "execute",
        "ts-news-service",
        "ts-notification-service",
        "ts-ticket-office-service",
        "ts-wait-order-service",
        "ts-food-delivery-service",
        "ts-delivery-service",
    ]

    # Path normalization patterns: (regex, replacement)
    _PATTERN_REPLACEMENTS = [
        (
            r"(.*?)GET (.*?)/api/v1/verifycode/verify/[0-9a-zA-Z]+",
            r"\1GET \2/api/v1/verifycode/verify/{verifyCode}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/foodservice/foods/[0-9]{4}-[0-9]{2}-[0-9]{2}/[a-z]+/[a-z]+/[A-Z0-9]+",
            r"\1GET \2/api/v1/foodservice/foods/{date}/{startStation}/{endStation}/{tripId}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/contactservice/contacts/account/[0-9a-f-]+",
            r"\1GET \2/api/v1/contactservice/contacts/account/{accountId}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/userservice/users/id/[0-9a-f-]+",
            r"\1GET \2/api/v1/userservice/users/id/{userId}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/consignservice/consigns/order/[0-9a-f-]+",
            r"\1GET \2/api/v1/consignservice/consigns/order/{id}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/consignservice/consigns/account/[0-9a-f-]+",
            r"\1GET \2/api/v1/consignservice/consigns/account/{id}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/executeservice/execute/collected/[0-9a-f-]+",
            r"\1GET \2/api/v1/executeservice/execute/collected/{orderId}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/cancelservice/cancel/[0-9a-f-]+/[0-9a-f-]+",
            r"\1GET \2/api/v1/cancelservice/cancel/{orderId}/{loginId}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/cancelservice/cancel/refound/[0-9a-f-]+",
            r"\1GET \2/api/v1/cancelservice/cancel/refound/{orderId}",
        ),
        (
            r"(.*?)GET (.*?)/api/v1/executeservice/execute/execute/[0-9a-f-]+",
            r"\1GET \2/api/v1/executeservice/execute/execute/{orderId}",
        ),
        (
            r"(.*?)DELETE (.*?)/api/v1/adminorderservice/adminorder/[0-9a-f-]+/[A-Z0-9]+",
            r"\1DELETE \2/api/v1/adminorderservice/adminorder/{orderId}/{trainNumber}",
        ),
        (
            r"(.*?)DELETE (.*?)/api/v1/adminrouteservice/adminroute/[0-9a-f-]+",
            r"\1DELETE \2/api/v1/adminrouteservice/adminroute/{routeId}",
        ),
    ]

    # Polars-compatible patterns (replace \1, \2 with ${1}, ${2})
    _PATTERN_REPLACEMENTS_POLARS = [
        (pat, rep.replace(r"\1", "${1}").replace(r"\2", "${2}")) for pat, rep in _PATTERN_REPLACEMENTS
    ]

    @property
    def black_list(self) -> list[str]:
        return self._BLACK_LIST

    @property
    def name(self) -> str:
        return "ts"

    @property
    def entrance_service(self) -> str:
        return "ts-ui-dashboard"

    def normalize_op_name(self, op_name: pl.Expr) -> pl.Expr:
        for pattern, replacement in self._PATTERN_REPLACEMENTS_POLARS:
            op_name = op_name.str.replace(pattern, replacement)
        return op_name

    def normalize_path(self, path: str) -> str:
        for pattern, replacement in self._PATTERN_REPLACEMENTS:
            result = re.sub(pattern, replacement, path)
            if result != path:
                return result
        return path

    def add_op_name(self, traces: pl.LazyFrame) -> pl.LazyFrame:
        op_name = pl.concat_str(pl.col("service_name"), pl.col("span_name"), separator=" ")
        return traces.with_columns(self.normalize_op_name(op_name).alias("op_name")).drop("span_name")

    @timeit(log_args=False)
    def fix_client_spans(self, traces: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, str], dict[str, str]]:
        id2op: dict[str, str] = {}
        id2parent: dict[str, str] = {}
        parent_child_map: defaultdict[str, set[str]] = defaultdict(set)

        # Build span relationship maps
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

        # Fix client spans and identify invalid ones
        to_delete = set()
        fix_client_spans: dict[str, str] = {}

        for span_id, op_name in id2op.items():
            if op_name.endswith("GET") or op_name.endswith("POST"):
                children = parent_child_map[span_id]
                if len(children) == 0:
                    # No child spans, mark for deletion
                    to_delete.add(span_id)
                elif len(children) == 1:
                    # Single child, merge path from child
                    child = children.pop()
                    child_op_name = id2op[child]
                    real_op_name = op_name + " " + child_op_name.split(" ")[2]
                    fix_client_spans[span_id] = real_op_name
                # Multiple children: keep as-is

        # Clean up deleted spans
        for span_id in to_delete:
            del id2op[span_id]
        id2op.update(fix_client_spans)
        del parent_child_map
        del to_delete

        # Apply fixes to DataFrame
        if len(fix_client_spans) > 0:
            client_spans_df = pl.DataFrame([{"span_id": sid, "op_name": op} for sid, op in fix_client_spans.items()])
            del fix_client_spans

            if debug():
                save_parquet(client_spans_df, path=get_config().temp / "sdg" / "client_spans.parquet")

            traces = traces.join(client_spans_df, on="span_id", how="left")
            traces = traces.with_columns(pl.coalesce("op_name_right", "op_name").alias("op_name"))

        return traces, id2op, id2parent
