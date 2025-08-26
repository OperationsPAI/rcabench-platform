import polars as pl

from ..algorithms.spec import global_algorithm_registry
from ..datasets.spec import get_datapack_list, get_dataset_meta_folder
from ..evaluation.ranking import calc_all_perf, calc_all_perf_by_datapack_attr
from ..logging import logger, timeit
from ..utils.dataframe import print_dataframe
from ..utils.serde import save_parquet
from .spec import get_output_folder, get_output_meta_folder


@timeit(log_level="INFO")
def generate_perf_report(dataset: str, *, warn_missing: bool = False, include_sampled: bool = False):
    datapacks = get_datapack_list(dataset)
    algorithms = list(global_algorithm_registry().keys())

    items = [(datapack, alg) for datapack in datapacks for alg in algorithms]

    output_paths = [get_output_folder(dataset, datapack, algorithm) / "output.parquet" for datapack, algorithm in items]

    # 如果include_sampled为True，还要查找包含sampler信息的output文件
    if include_sampled:
        for datapack in datapacks:
            # 查找所有包含sampler信息的算法目录
            base_output_folder = get_output_folder(dataset, datapack, "dummy").parent
            if base_output_folder.exists():
                for algorithm_dir in base_output_folder.iterdir():
                    if algorithm_dir.is_dir() and "_sampled_" in algorithm_dir.name:
                        output_file = algorithm_dir / "output.parquet"
                        if output_file.exists():
                            output_paths.append(output_file)

    valid_output_paths = []
    for path in output_paths:
        if path.exists():
            valid_output_paths.append(path)
        elif warn_missing:
            logger.warning(f"missing output file: {path}")

    assert len(valid_output_paths) > 0, f"No output files found for dataset `{dataset}`. "

    logger.debug(f"loading {len(valid_output_paths)} output files")
    output_df = pl.read_parquet(valid_output_paths, rechunk=True)

    output_meta_folder = get_output_meta_folder(dataset)
    save_parquet(output_df, path=output_meta_folder / "output.parquet")

    if dataset.startswith("rcabench"):
        attributes_df_path = get_dataset_meta_folder(dataset) / "attributes.parquet"
        if attributes_df_path.exists():
            attr_col = "injection.fault_type"
            attr_df = pl.read_parquet(attributes_df_path, columns=["datapack", attr_col])
            attr_df = output_df.join(attr_df, on="datapack", how="left")
            perf_df = calc_all_perf_by_datapack_attr(attr_df, dataset, attr_col)
            save_parquet(perf_df, path=output_meta_folder / "fault_types.perf.parquet")
    elif dataset.startswith("rcaeval"):
        attributes_df_path = get_dataset_meta_folder(dataset) / "attributes.parquet"
        if attributes_df_path.exists():
            attr_col = "fault_type"
            attr_df = pl.read_parquet(attributes_df_path, columns=["datapack", attr_col])
            attr_df = output_df.join(attr_df, on="datapack", how="left")
            perf_df = calc_all_perf_by_datapack_attr(attr_df, dataset, attr_col)
            save_parquet(perf_df, path=output_meta_folder / "fault_types.perf.parquet")
    elif dataset.startswith("aiops21"):
        attributes_df_path = get_dataset_meta_folder(dataset) / "attributes.parquet"
        if attributes_df_path.exists():
            attr_col = "injection.fault_type"
            attr_df = pl.read_parquet(attributes_df_path, columns=["datapack", attr_col])
            attr_df = output_df.join(attr_df, on="datapack", how="left")
            perf_df = calc_all_perf_by_datapack_attr(attr_df, dataset, attr_col)
            save_parquet(perf_df, path=output_meta_folder / "fault_types.perf.parquet")

    perf_df = calc_all_perf(output_df, agg_level="datapack")
    save_parquet(perf_df, path=output_meta_folder / "datapack.perf.parquet")

    perf_df = calc_all_perf(output_df, agg_level="dataset")
    save_parquet(perf_df, path=output_meta_folder / "dataset.perf.parquet")

    if include_sampled:
        # 如果包含sampled数据，显示包含sampler字段的完整报告
        print_dataframe(
            perf_df.select(
                "dataset",
                "algorithm",
                "sampler.name",
                "sampler.rate",
                "sampler.mode",
                "total",
                "error",
                "runtime.seconds:avg",
                "MRR",
                "AC@1.count",
                "AC@3.count",
                "AC@5.count",
                "AC@1",
                "AC@3",
                "AC@5",
                "Avg@3",
                "Avg@5",
            ).with_columns(
                pl.when(pl.col("sampler.rate").is_not_null())
                .then(pl.col("sampler.rate").round(3))
                .otherwise(pl.col("sampler.rate"))
                .alias("sampler.rate")
            )
        )

        # 保存包含sampler信息的报告
        sampled_df = output_df.filter(pl.col("sampler.name").is_not_null())
        if len(sampled_df) > 0:
            sampler_perf_df = calc_all_perf(sampled_df, agg_level="dataset")
            save_parquet(sampler_perf_df, path=output_meta_folder / "sampler.dataset.perf.parquet")
    else:
        # 如果不包含sampled数据，显示原来的报告格式
        print_dataframe(
            perf_df.select(
                "dataset",
                "algorithm",
                "total",
                "error",
                "runtime.seconds:avg",
                "MRR",
                "AC@1.count",
                "AC@3.count",
                "AC@5.count",
                "AC@1",
                "AC@3",
                "AC@5",
                "Avg@3",
                "Avg@5",
            )
        )
