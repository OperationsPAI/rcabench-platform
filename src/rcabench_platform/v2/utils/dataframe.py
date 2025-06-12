import polars as pl


def assert_columns(df: pl.DataFrame, cols: list[str]) -> None:
    for col in cols:
        assert col in df.columns, f"Column {col} not found in DataFrame"


def print_dataframe(df: pl.DataFrame) -> None:
    with pl.Config(
        tbl_rows=len(df),
        tbl_cols=len(df.columns),
        fmt_str_lengths=120,
        tbl_width_chars=160,
        tbl_cell_numeric_alignment="RIGHT",
    ):
        print(df)
