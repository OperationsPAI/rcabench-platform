"""Initialize and register default sampler algorithms."""

from .random_ import RandomSampler
from .spec import register_sampler


def _register_default_samplers():
    """Register default sampler algorithms."""
    register_sampler("random", lambda: RandomSampler())


# Automatically register default samplers when module is imported
_register_default_samplers()
