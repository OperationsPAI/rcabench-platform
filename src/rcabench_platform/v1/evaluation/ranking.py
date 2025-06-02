from ..utils.dataframe import assert_columns

from typing import Literal

import polars as pl

INDEX_COLUMNS = ["algorithm", "dataset", "datapack"]
AGG_LEVEL = Literal["algorithm", "dataset", "datapack"]


def agg_index(agg_level: AGG_LEVEL):
    if agg_level == "datapack":
        index_columns = INDEX_COLUMNS
    elif agg_level == "dataset":
        index_columns = INDEX_COLUMNS[:-1]
    elif agg_level == "algorithm":
        index_columns = INDEX_COLUMNS[:-2]
    else:
        raise ValueError(f"Invalid agg_level: {agg_level}")

    return index_columns


def calc_avg_runtime(df: pl.DataFrame, agg_level: AGG_LEVEL) -> pl.DataFrame:
    lf = df.lazy()

    lf = lf.select(*INDEX_COLUMNS, pl.col("runtime.seconds"))

    lf = lf.group_by(INDEX_COLUMNS).agg(pl.col("runtime.seconds").max())

    lf = lf.group_by(agg_index(agg_level)).agg(pl.col("runtime.seconds").mean().round(6).alias("runtime.seconds:avg"))

    df = lf.collect()
    return df


def calc_mrr(df: pl.DataFrame, agg_level: AGG_LEVEL) -> pl.DataFrame:
    """
    MRR: Mean Reciprocal Rank

    https://en.wikipedia.org/wiki/Mean_reciprocal_rank
    """

    lf = df.lazy()

    lf = lf.filter(pl.col("hit"))

    lf = lf.select(*INDEX_COLUMNS, pl.col("rank"))

    lf = lf.group_by(INDEX_COLUMNS).agg(pl.col("rank").min().alias("rank"))

    lf = lf.with_columns((1 / pl.col("rank")).alias("MRR"))

    agg_cols = [pl.col("MRR").mean().round(6)]

    if agg_level == "datapack":
        agg_cols = [pl.col("rank").first(), *agg_cols]

    lf = lf.group_by(agg_index(agg_level)).agg(*agg_cols)

    df = lf.collect()
    return df


def calc_accurary(df: pl.DataFrame, agg_level: AGG_LEVEL) -> pl.DataFrame:
    """
    AC@k: The probability that the top k results contain at least one relevant answer.

    Avg@k = sum(AC@i for i in 1..=i) / k

    https://dl.acm.org/doi/pdf/10.1145/3691620.3695065 S3.2.2
    """

    K = 5
    rangeK = range(1, K + 1)

    lf = df.lazy()

    hit_k = [(pl.col("hit") & (pl.col("rank") <= k)).alias(f"hit@{k}") for k in rangeK]

    lf = lf.select(*INDEX_COLUMNS, *hit_k)

    lf = lf.group_by(INDEX_COLUMNS).agg([pl.col(f"hit@{k}").any().cast(pl.Float64).alias(f"AC@{k}") for k in rangeK])

    lf = lf.group_by(agg_index(agg_level)).agg(
        *[pl.col(f"AC@{k}").sum().alias(f"AC@{k}.count") for k in rangeK],
        *[pl.col(f"AC@{k}").mean().alias(f"AC@{k}").round(6) for k in rangeK],
    )

    avg_k = [pl.mean_horizontal(pl.col(f"AC@{i}") for i in range(1, k + 1)).round(6).alias(f"Avg@{k}") for k in rangeK]

    lf = lf.with_columns(*avg_k)

    df = lf.collect()
    return df


def calc_precision(df: pl.DataFrame, agg_level: AGG_LEVEL) -> pl.DataFrame:
    """
    P@k: Precision at k.

    AP@k: Average Precision at k.

    https://sdsawtelle.github.io/blog/output/mean-average-precision-MAP-for-recommender-systems.html
    """

    K = 5
    rangeK = range(1, K + 1)

    lf = df.lazy()

    hit_k = [(pl.col("hit") & (pl.col("rank") <= k)).alias(f"hit@{k}") for k in rangeK]
    rel_k = [(pl.col("hit") & (pl.col("rank") == k)).alias(f"rel@{k}") for k in rangeK]

    lf = lf.select(*INDEX_COLUMNS, *hit_k, *rel_k)

    p_k = [pl.col(f"hit@{k}").sum().cast(pl.Float64).truediv(k).alias(f"P@{k}") for k in rangeK]

    rel_k = [pl.col(f"rel@{k}").any().cast(pl.Float64).alias(f"rel@{k}") for k in rangeK]

    hit_k = [pl.col(f"hit@{k}").sum().cast(pl.Float64).alias(f"hit@{k}") for k in rangeK]

    lf = lf.group_by(INDEX_COLUMNS).agg(*p_k, *rel_k, *hit_k)

    ap_k = [
        (
            pl.sum_horizontal(pl.col(f"P@{i}") * pl.col(f"rel@{i}") for i in range(1, k + 1))
            .truediv(pl.col(f"hit@{k}"))
            .fill_nan(0)
            .round(6)
            .alias(f"AP@{k}")
        )
        for k in rangeK
    ]

    lf = (
        lf.with_columns(*ap_k)
        .drop([f"rel@{k}" for k in rangeK], strict=True)
        .drop([f"hit@{k}" for k in rangeK], strict=True)
    )

    if agg_level != "datapack":
        map_k = [pl.col(f"AP@{k}").mean().round(6).alias(f"MAP@{k}") for k in rangeK]
        lf = lf.group_by(agg_index(agg_level)).agg(*map_k)

    df = lf.collect()
    return df


def calc_index(df: pl.DataFrame, agg_level: AGG_LEVEL) -> pl.DataFrame:
    lf = df.lazy()

    lf = lf.with_columns(pl.col("exception.type").is_not_null().cast(pl.UInt32).alias("error"))

    lf = lf.select(*INDEX_COLUMNS, "error").unique(subset=INDEX_COLUMNS)

    lf = lf.group_by(agg_index(agg_level)).agg(
        pl.len().cast(pl.UInt32).alias("total"),
        pl.col("error").sum().cast(pl.UInt32).alias("error"),
    )

    df = lf.collect()
    return df


def calc_all_perf(df: pl.DataFrame, *, agg_level: AGG_LEVEL) -> pl.DataFrame:
    assert_columns(df, INDEX_COLUMNS)
    assert_columns(df, ["hit", "rank", "runtime.seconds", "exception.type"])

    index = agg_index(agg_level)

    ans = calc_index(df, agg_level)

    ans = ans.join(calc_avg_runtime(df, agg_level), on=index, how="left")
    ans = ans.join(calc_mrr(df, agg_level), on=index, how="left")
    ans = ans.join(calc_accurary(df, agg_level), on=index, how="left")
    ans = ans.join(calc_precision(df, agg_level), on=index, how="left")

    ans = ans.fill_null(strategy="zero")

    if agg_level == "datapack":
        ans = ans.sort(by=["algorithm", "rank", "dataset", "datapack"])
    elif agg_level == "dataset":
        ans = ans.sort(by=["algorithm", "dataset"])

    return ans


def calc_all_perf_by_datapack_attr(df: pl.DataFrame, dataset: str, attr_col: str) -> pl.DataFrame:
    assert df.filter(pl.col("dataset") != dataset).is_empty()

    df = df.drop("dataset").with_columns(pl.col(attr_col).alias("dataset"))

    perf_df = calc_all_perf(df, agg_level="dataset")

    perf_df = perf_df.with_columns(pl.col("dataset").alias(attr_col))

    perf_df = perf_df.select(
        "algorithm",
        pl.lit(dataset).alias("dataset"),
        attr_col,
        pl.all().exclude("algorithm", "dataset", attr_col),
    )

    return perf_df
