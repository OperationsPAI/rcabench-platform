"""Random trace sampling algorithm implementation."""

import random
from pathlib import Path

import polars as pl

from .spec import SamplerArgs, SampleResult, SamplingMode, TraceSampler


class RandomSampler(TraceSampler):
    """Random trace sampling algorithm that randomly samples traces."""

    def __init__(self, seed: int | None = None):
        """
        Initialize the random sampler.

        Args:
            seed: Random seed for reproducible results. If None, uses random seed.
        """
        self.seed = seed

    def needs_cpu_count(self) -> int | None:
        """Random sampler only needs a single CPU core."""
        return 1

    def __call__(self, args: SamplerArgs) -> list[SampleResult]:
        """
        Execute random trace sampling.

        Args:
            args: Sampler arguments

        Returns:
            List of SampleResult with random scores for traces.
        """
        # Set random seed if provided
        if self.seed is not None:
            random.seed(self.seed)

        # Load traces data to get all trace_ids
        traces_file = args.input_folder / "traces.parquet"
        if not traces_file.exists():
            raise FileNotFoundError(f"Traces file not found: {traces_file}")

        # Read unique trace_ids from traces
        traces_lf = pl.scan_parquet(traces_file)
        unique_traces = traces_lf.select("trace_id").unique().collect()
        trace_ids = unique_traces["trace_id"].to_list()

        # Generate random scores for all traces
        all_results = []
        for trace_id in trace_ids:
            sample_score = random.random()  # Random score between 0.0 and 1.0
            all_results.append(SampleResult(trace_id=trace_id, sample_score=sample_score))

        # Sort by score (higher scores first)
        all_results.sort(key=lambda x: x.sample_score, reverse=True)

        # Apply sampling mode
        if args.mode == SamplingMode.ONLINE:
            # Online mode: return all traces with their scores
            return all_results
        elif args.mode == SamplingMode.OFFLINE:
            # Offline mode: limit by sampling rate
            total_traces = len(all_results)
            target_count = int(total_traces * args.sampling_rate)
            return all_results[:target_count]
        else:
            raise ValueError(f"Unknown sampling mode: {args.mode}")


def create_random_sampler(seed: int | None = None) -> RandomSampler:
    """Factory function to create a RandomSampler instance."""
    return RandomSampler(seed=seed)
