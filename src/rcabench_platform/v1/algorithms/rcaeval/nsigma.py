from ._common import SimpleMetricsAdapter
from ...spec.algorithm import Algorithm, AlgorithmArgs, AlgorithmAnswer

from ....vendor.RCAEval.nsigma import nsigma


class NSigma(Algorithm):
    def needs_cpu_count(self) -> int | None:
        return 4

    def __call__(self, args: AlgorithmArgs) -> list[AlgorithmAnswer]:
        adapter = SimpleMetricsAdapter(nsigma)
        return adapter(args)
