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

    # If include_sampled is True, also look for output files containing sampler information
    if include_sampled:
        for datapack in datapacks:
            # Find all algorithm directories containing sampler information
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

    # Generate regular performance reports (excluding sampled data)
    perf_df = calc_all_perf(output_df, agg_level="datapack", include_sampled=False)
    save_parquet(perf_df, path=output_meta_folder / "datapack.perf.parquet")

    perf_df = calc_all_perf(output_df, agg_level="dataset", include_sampled=False)
    save_parquet(perf_df, path=output_meta_folder / "dataset.perf.parquet")

    if include_sampled:
        # Check if there's any sampled data
        if "sampler.name" in output_df.columns:
            sampled_df = output_df.filter(pl.col("sampler.name").is_not_null())
        else:
            sampled_df = pl.DataFrame()

        if len(sampled_df) > 0:
            # Generate detailed sampler performance report (all datapack details)
            sampler_detailed_perf = calc_all_perf(output_df, agg_level="sampler", include_sampled=True)
            save_parquet(sampler_detailed_perf, path=output_meta_folder / "sampler.detailed.perf.parquet")

            # Generate sampler performance aggregated at dataset level
            sampler_agg_perf = calc_all_perf(output_df, agg_level="sampler_dataset", include_sampled=True)
            save_parquet(sampler_agg_perf, path=output_meta_folder / "sampler.aggregated.perf.parquet")

            # Use the same calc_all_perf function to generate sampler performance report
            # This ensures consistency with regular performance metrics
            sampler_grouped_perf = calc_all_perf(output_df, agg_level="sampler_dataset", include_sampled=True)

            save_parquet(sampler_grouped_perf, path=output_meta_folder / "sampler.grouped.perf.parquet")

            # Display sampler performance report with same format as regular report
            display_columns = [
                "algorithm",
                "dataset",
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
            ]

            # Filter columns that actually exist in the dataframe
            available_columns = [col for col in display_columns if col in sampler_grouped_perf.columns]

            print("=== Sampler Performance Report (Algorithm + Sampler Aggregation) ===")
            print_dataframe(sampler_grouped_perf.select(available_columns))
        else:
            logger.info("No sampled data found in the output files")

        # Also display the regular (non-sampled) report for comparison
        print("=== Regular Performance Report ===")
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
    else:
        # If sampled data is not included, display the original report format
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
