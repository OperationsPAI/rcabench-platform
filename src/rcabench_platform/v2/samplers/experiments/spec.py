"""Specification for sampler experiments."""

from pathlib import Path

from ...config import get_config
from ..spec import SamplingMode


def get_sampler_output_folder(
    dataset: str, datapack: str, sampler: str, sampling_rate: float, mode: SamplingMode
) -> Path:
    """
    Get the output folder for sampler results.

    Format: {output}/sampled/{dataset}/{datapack}/{sampler}_{sampling_rate}_{mode}
    """
    config = get_config()
    mode_str = mode.value
    rate_str = f"{sampling_rate:.3f}".rstrip("0").rstrip(".")
    folder_name = f"{sampler}_{rate_str}_{mode_str}"
    return config.output / "sampled" / dataset / datapack / folder_name
