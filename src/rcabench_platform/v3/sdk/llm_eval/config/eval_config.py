from typing import Literal

from pydantic import Field

from ..utils import EnvUtils
from .base_config import ConfigBaseModel
from .model_config import ModelConfigs


class DataConfig(ConfigBaseModel):
    """Data config"""

    dataset: str  # RCABench or custom dataset path
    """Built-in dataset name or custom dataset path"""
    type: Literal["single", "mixed"] = "single"
    """Whether the dataset contains only single benchmark data or multiple benchmarks"""
    question_field: str = "question"
    """Question field name in the dataset"""
    gt_field: str = "answer"
    """Ground truth field name in the dataset"""
    tags: list[str] | None = None
    """Filter samples by tags (OR logic: any matching tag)"""


class EvalConfig(ConfigBaseModel):
    """Evaluation config"""

    exp_id: str = "default"
    """Experiment ID"""

    # data
    db_url: str | None = EnvUtils.get_env("LLM_EVAL_DB_URL", None) or EnvUtils.get_env("UTU_DB_URL", "sqlite:///test.db")
    """Database URL"""
    data: DataConfig | None = None
    """Data config"""

    # rollout
    agent_type: str | None = None
    """Agent type label for tracking"""
    model_name: str | None = None
    """Model name label for tracking"""
    concurrency: int = 1
    """Rollout parallelism"""
    max_samples: int | None = None
    """Maximum number of samples to rollout (None = all)"""

    # judgement
    judge_model: ModelConfigs = Field(default_factory=ModelConfigs)
    """Judge model config"""
    judge_concurrency: int = 1
    """Judgement parallelism"""
    eval_method: str | None = None
    """Evaluation method"""
    pass_k: int = 1
    """Number of attempts to consider for pass@k metrics"""
