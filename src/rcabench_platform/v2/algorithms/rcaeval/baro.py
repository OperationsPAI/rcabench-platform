from ._common import SimpleMetricsAdapter
from ...algorithms.spec import Algorithm, AlgorithmArgs, AlgorithmAnswer

from ....vendor.RCAEval.baro import baro


class Baro(Algorithm):
    def needs_cpu_count(self) -> int | None:
        return 4

    def __call__(self, args: AlgorithmArgs) -> list[AlgorithmAnswer]:
        adapter = SimpleMetricsAdapter(baro)
        return adapter(args)
