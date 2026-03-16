import asyncio
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal

from tqdm import tqdm

from ...config import ConfigLoader, EvalConfig
from ...utils import get_logger
from ..data import DBDataManager, EvaluationSample
from ..processer import PROCESSER_FACTORY, BaseProcesser

logger = get_logger(__name__, "INFO")


class BaseBenchmark:
    """Base class for benchmarks.

    Evaluation phases:
      - preprocess: load and preprocess the data
      - judge: judge the correctness of a batch of predictions
      - stat: get metrics.
    """

    dataset: DBDataManager
    _source_to_processer: dict[str, BaseProcesser] = {}

    def __init__(self, config: EvalConfig | str) -> None:
        # config
        if isinstance(config, str):
            config = ConfigLoader.load_eval_config(path=config)
        self.config = config

        # dataset
        self.dataset = DBDataManager(config)
        _samples = self.dataset.load()
        if len(_samples) == 0:
            raise ValueError(f"No samples found for data config '{self.config.data}'! Please check the data config.")

    @property
    def agent_type(self) -> str | None:
        """Get agent type from config."""
        return self.config.agent_type

    @property
    def model_name(self) -> str | None:
        """Get model name from config."""
        return self.config.model_name

    @property
    def tags(self) -> list[str] | None:
        """Get tags from config."""
        if self.config.data:
            return self.config.data.tags
        return None

    async def main(self):
        logger.info(f"> Running with config: \n{json.dumps(self.config.model_dump(), indent=2, ensure_ascii=False)}")
        self.preprocess()
        await self.judge()
        logger.info("> Running stat...")
        await self.stat()

    def preprocess(self) -> list[EvaluationSample]:
        """Preprocess the dataset before rollout."""
        samples = self.dataset.get_samples(
            stage="init", agent_type=self.agent_type, model_name=self.model_name, tags=self.tags
        )
        if self.config.max_samples is not None:
            samples = samples[: self.config.max_samples]
        logger.info(f"Preprocessing {len(samples)} samples...")

        results = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.preprocess_one, sample): sample for sample in samples}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Preprocessing"):
                result = future.result()
                if result is not None:
                    results.append(result)

        self.dataset.save(results)
        logger.info(f"Successfully preprocessed {len(results)} samples. Updated to db.")
        return results

    def preprocess_one(self, sample: EvaluationSample) -> EvaluationSample | None:
        processer = self._get_processer(sample.dataset)
        processed_sample = processer.preprocess_one(sample)
        if processed_sample is None:
            return None
        return sample

    def submit_result(
        self,
        sample_id: int | None = None,
        dataset_index: int | None = None,
        response: str = "",
        trajectory_json: str | None = None,
        time_cost: float | None = None,
        trace_id: str | None = None,
    ) -> EvaluationSample:
        """Submit an agent result for a sample.

        Args:
            sample_id: DB primary key of the EvaluationSample. Use this OR dataset_index.
            dataset_index: Dataset index to identify the sample. Use this OR sample_id.
            response: Agent's final response/output.
            trajectory_json: JSON string of agent trajectory.
            time_cost: Wall-clock time for the rollout.
            trace_id: Optional trace ID for observability.

        Returns:
            Updated EvaluationSample.
        """
        if sample_id is not None:
            samples = [s for s in self.dataset.data if s.id == sample_id]
        elif dataset_index is not None:
            samples = [s for s in self.dataset.data if s.dataset_index == dataset_index]
        else:
            raise ValueError("Must provide either sample_id or dataset_index")

        if not samples:
            raise ValueError(f"Sample not found (sample_id={sample_id}, dataset_index={dataset_index})")

        sample = samples[0]
        sample.update(
            response=response,
            trajectories=trajectory_json,
            time_cost=time_cost,
            trace_id=trace_id,
            stage="rollout",
        )
        self.dataset.save(sample)
        return sample

    async def judge(self, stage: Literal["init", "rollout", "judged"] | None = "rollout") -> list[EvaluationSample]:
        """Judge samples.

        Args:
            stage (str|None, optional): The stage of samples to judge. If set to None, you can rejudge all samples.
        """
        samples = self.dataset.get_samples(
            stage=stage, agent_type=self.agent_type, model_name=self.model_name, tags=self.tags
        )
        logger.info(f"Judging {len(samples)} samples...")

        semaphore = asyncio.Semaphore(self.config.judge_concurrency)

        async def judge_with_semaphore(item: EvaluationSample):
            async with semaphore:
                try:
                    return await self.judge_one(item)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error(f">>>>>>>>>>>>>\nError judging sample '{item}': {e}\n<<<<<<<<<<<<<", exc_info=True)
                    return None

        tasks = [judge_with_semaphore(item) for item in samples]
        results = []
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Judging"):
            result = await task
            if result is not None:
                results.append(result)
        logger.info(f"Successfully judged {len(results)} samples. Updated to db.")
        return results

    async def judge_one(self, data: EvaluationSample) -> EvaluationSample:
        judger = self._get_processer(data.dataset)
        result = await judger.judge_one(data)
        result.update(stage="judged")  # update stage to judged
        self.dataset.save(result)
        return result

    async def stat(self) -> list[dict]:
        judged_samples = self.dataset.get_samples(
            stage="judged", agent_type=self.agent_type, model_name=self.model_name, tags=self.tags
        )
        logger.info(f"Stat from {len(judged_samples)} samples (agent={self.agent_type}, model={self.model_name}):")

        data_by_benchmark = self._group_data_by_benchmark(judged_samples)
        overall_results: list[dict] = []
        for benchmark, data in data_by_benchmark.items():
            evaluator = self._get_processer(benchmark)
            result = await evaluator.stat(data)
            result["agent_type"] = self.agent_type
            result["model_name"] = self.model_name
            overall_results.append(result)

        logger.info(json.dumps(overall_results, indent=4, ensure_ascii=False))
        print(json.dumps(overall_results, indent=4, ensure_ascii=False))
        return overall_results

    def _get_processer(self, source: str) -> BaseProcesser:
        if source not in self._source_to_processer:
            processer = PROCESSER_FACTORY.get(source, self.config)
            self._source_to_processer[source] = processer
        return self._source_to_processer[source]

    def _group_data_by_benchmark(self, predict_data: list[EvaluationSample]) -> dict[str, list[EvaluationSample]]:
        data_by_benchmark: dict[str, list[EvaluationSample]] = {}
        for data in predict_data:
            benchmark = data.dataset
            if benchmark not in data_by_benchmark:
                data_by_benchmark[benchmark] = []
            data_by_benchmark[benchmark].append(data)
        return data_by_benchmark
