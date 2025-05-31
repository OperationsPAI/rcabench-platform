import polars as pl


def is_constant_metric(df: pl.DataFrame) -> bool:
    col = pl.col("value")
    df = df.select(min=col.min(), max=col.max())
    min_value, max_value = df.row(0)
    assert isinstance(min_value, float)
    assert isinstance(max_value, float)
    return (max_value - min_value) < 1e-8


def replace_enum_values(lf: pl.LazyFrame, col_name: str, values: list[str]) -> pl.LazyFrame:
    lf = lf.with_columns(
        pl.col(col_name)
        .cast(pl.Enum(values))
        .replace_strict({value: i for i, value in enumerate(values, start=1)})
        .cast(pl.Float64)
    )
    return lf
