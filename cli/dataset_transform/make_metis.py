#!/usr/bin/env -S uv run -s
from pathlib import Path

from rcabench_platform.v2.cli.main import app, timeit
from rcabench_platform.v2.logging import logger
from rcabench_platform.v2.sources.convert import convert_dataset
from rcabench_platform.v2.sources.metis import MetisDatasetLoader


@app.command()
@timeit()
def run(
    skip_finished: bool = True,
    parallel: int = 4,
):
    src_root = Path("metis")

    # Convert TS
    logger.info("Converting Metis TS dataset...")
    ts_loader = MetisDatasetLoader(src_root, sub_folder="ts", dataset="metis-ts")
    convert_dataset(
        ts_loader,
        skip_finished=skip_finished,
        parallel=parallel,
        ignore_exceptions=True,
    )

    # Convert OB
    logger.info("Converting Metis OB dataset...")
    ob_loader = MetisDatasetLoader(src_root, sub_folder="ob", dataset="metis-ob")
    convert_dataset(
        ob_loader,
        skip_finished=skip_finished,
        parallel=parallel,
        ignore_exceptions=True,
    )


@app.command()
@timeit()
def build_template():
    """Build log templates for all datapacks using Drain3."""
    dataset = MetisDatasetLoader(Path("metis"), sub_folder="ts", dataset="metis-ts")

    datapacks = dataset._datapacks
    logger.info(f"Found {len(datapacks)} datapacks for template building")
    import polars as pl
    from tqdm import tqdm

    from rcabench_platform.v2.sources.rcabench import create_template_miner

    def extract_unique_log_messages(src_root: Path, datapacks: list[str]) -> pl.DataFrame:
        """Extract unique log messages from all datapacks, excluding ts-ui-dashboard service."""
        all_logs = []

        for datapack in datapacks:
            datapack_folder = src_root / datapack
            for prefix in ("normal", "abnormal"):
                log_file = datapack_folder / prefix / "logs.csv"
                if log_file.exists():
                    lf = pl.scan_csv(log_file).select("Body", "ServiceName")
                    all_logs.append(lf)

        if not all_logs:
            return pl.DataFrame(schema={"Body": pl.String})

        # Combine all logs and filter out ts-ui-dashboard
        combined_lf = pl.concat(all_logs)
        unique_messages = (
            combined_lf.filter(pl.col("ServiceName") != "ts-ui-dashboard").select("Body").unique().collect()
        )

        return unique_messages

    # Extract unique messages from all datapacks
    unique_messages = extract_unique_log_messages(Path("metis/ts"), datapacks)
    logger.info(f"Extracted {unique_messages.height} unique log messages")

    if unique_messages.height == 0:
        logger.warning("No log messages found for template processing")
        return

    template_base = Path("metis") / "drain_template"

    config_path = template_base / "drain_ts.ini"
    persistence_path = template_base / "drain_ts.bin"

    logger.info(f"Using template paths: config={config_path}, persistence={persistence_path}")

    template_miner = create_template_miner(config_path, persistence_path)

    logger.info("Processing all unique log messages with Drain3...")
    processed_count = 0

    for message in tqdm(unique_messages["Body"].to_list(), desc="Processing messages"):
        if message:  # Skip empty messages
            template_miner.add_log_message(message)
            processed_count += 1

    logger.info(f"Processed {processed_count} messages and built {len(template_miner.drain.clusters)} templates")
    logger.info(f"Template state saved to {persistence_path}")
@app.command()
@timeit()
def build_template_ob():
    """Build log templates for Metis OB dataset using Drain3."""
    dataset = MetisDatasetLoader(Path("metis"), sub_folder="ob", dataset="metis-ob")

    datapacks = dataset._datapacks
    logger.info(f"Found {len(datapacks)} datapacks for template building")
    import polars as pl
    from tqdm import tqdm

    from rcabench_platform.v2.sources.rcabench import create_template_miner

    def extract_unique_log_messages(src_root: Path, datapacks: list[str]) -> pl.DataFrame:
        """Extract unique log messages from all datapacks."""
        all_logs = []

        for datapack in datapacks:
            datapack_folder = src_root / datapack
            for prefix in ("normal", "abnormal"):
                log_file = datapack_folder / prefix / "logs.csv"
                if log_file.exists():
                    lf = pl.scan_csv(log_file).select("Body")
                    all_logs.append(lf)

        if not all_logs:
            return pl.DataFrame(schema={"Body": pl.String})

        # Combine all logs
        combined_lf = pl.concat(all_logs)
        unique_messages = combined_lf.select("Body").unique().collect()

        return unique_messages

    # Extract unique messages from all datapacks
    unique_messages = extract_unique_log_messages(Path("metis/ob"), datapacks)
    logger.info(f"Extracted {unique_messages.height} unique log messages")

    if unique_messages.height == 0:
        logger.warning("No log messages found for template processing")
        return

    template_base = Path("metis") / "drain_template"

    config_path = template_base / "drain_ts.ini"
    persistence_path = template_base / "drain_ts.bin"

    logger.info(f"Using template paths: config={config_path}, persistence={persistence_path}")

    template_miner = create_template_miner(config_path, persistence_path)

    logger.info("Processing all unique log messages with Drain3...")
    processed_count = 0

    for message in tqdm(unique_messages["Body"].to_list(), desc="Processing messages"):
        if message:  # Skip empty messages
            template_miner.add_log_message(message)
            processed_count += 1

    logger.info(f"Processed {processed_count} messages and built {len(template_miner.drain.clusters)} templates")
    logger.info(f"Template state saved to {persistence_path}")
if __name__ == "__main__":
    app()
