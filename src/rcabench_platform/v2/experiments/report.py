from .spec import get_output_folder, get_output_meta_folder

from ..utils.dataframe import print_dataframe
from ..evaluation.ranking import calc_all_perf, calc_all_perf_by_datapack_attr
from ..utils.serde import save_parquet
from ..algorithms.spec import global_algorithm_registry
from ..datasets.spec import get_dataset_meta_folder, get_datapack_list
from ..logging import logger, timeit

import polars as pl


@timeit(log_level="INFO")
def generate_perf_report(dataset: str, *, warn_missing: bool = False):
    datapacks = get_datapack_list(dataset)
    algorithms = list(global_algorithm_registry().keys())

    items = [(datapack, alg) for datapack in datapacks for alg in algorithms]

    output_paths = [get_output_folder(dataset, datapack, algorithm) / "output.parquet" for datapack, algorithm in items]

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
    elif dataset.startswith("rcaeval"):
        attributes_df_path = get_dataset_meta_folder(dataset) / "attributes.parquet"
        if attributes_df_path.exists():
            attr_col = "fault_type"
            attr_df = pl.read_parquet(attributes_df_path, columns=["datapack", attr_col])
            attr_df = output_df.join(attr_df, on="datapack", how="left")
            perf_df = calc_all_perf_by_datapack_attr(attr_df, dataset, attr_col)
            save_parquet(perf_df, path=output_meta_folder / "fault_types.perf.parquet")

    perf_df = calc_all_perf(output_df, agg_level="datapack")
    save_parquet(perf_df, path=output_meta_folder / "datapack.perf.parquet")

    perf_df = calc_all_perf(output_df, agg_level="dataset")
    save_parquet(perf_df, path=output_meta_folder / "dataset.perf.parquet")

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
        )
    )
