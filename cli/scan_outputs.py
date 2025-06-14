#!/usr/bin/env -S uv run -s
from rcabench_platform.v2.cli.main import app, logger, timeit
from rcabench_platform.v2.datasets.spec import get_datapack_list
from rcabench_platform.v2.experiments.report import get_output_meta_folder
from rcabench_platform.v2.experiments.single import get_output_folder
from rcabench_platform.v2.utils.serde import save_parquet

from fractions import Fraction

import polars as pl


@app.command()
@timeit()
def scan_rcaeval_ranks(dataset: str, algorithm: str):
    datapacks = get_datapack_list(dataset)

    lf_list: list[pl.LazyFrame] = []
    for datapack in datapacks:
        output_folder = get_output_folder(dataset, datapack, algorithm)
        lf = pl.scan_parquet(output_folder / "ranks.parquet")
        lf = lf.select(pl.lit(dataset).alias("dataset"), pl.lit(datapack).alias("datapack"), pl.all())
        lf_list.append(lf)

    ranks = pl.concat(lf_list).collect()
    save_parquet(ranks, path=get_output_meta_folder(dataset) / f"{algorithm}.ranks.parquet")

    stats_data = []
    for metric in ["latency", "cpu", "mem", "diskio", "workload"]:
        for rank in range(1, 5 + 1):
            df = ranks.filter(pl.col("rank") <= rank)
            total_count = len(df)

            df = df.filter(pl.col("node_name").str.contains(metric))
            metric_count = len(df)

            prop = Fraction(metric_count, total_count)

            stats_data.append(
                {
                    "metric": metric,
                    "rank": rank,
                    "metric_count": metric_count,
                    "total_count": total_count,
                    "proportion": round(float(prop), 6),
                }
            )

    stats_df = pl.DataFrame(stats_data)
    save_parquet(stats_df, path=get_output_meta_folder(dataset) / f"{algorithm}.ranks_stats.parquet")


if __name__ == "__main__":
    app()
