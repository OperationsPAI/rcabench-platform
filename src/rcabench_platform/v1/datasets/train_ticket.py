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


def normalize_op_name_by_polars(op_name: pl.Expr) -> pl.Expr:
    for pattern, replacement in PATTERN_REPLACEMENTS_POLARS:
        op_name = op_name.str.replace(pattern, replacement)
    return op_name
