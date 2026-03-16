"""CLI subcommand for LLM agent evaluation.

Usage:
    rca llm-eval run <config_path>       -- preprocess + judge + stat
    rca llm-eval judge <config_path>     -- re-judge existing rollouts
    rca llm-eval stat <config_path>      -- show metrics only
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Literal, cast

import typer

app = typer.Typer(help="LLM agent evaluation commands", pretty_exceptions_show_locals=False)


def _load_benchmark(config_path: str, exp_id: str | None = None):
    """Load eval config and create benchmark instance."""
    from ..sdk.llm_eval.config import ConfigLoader
    from ..sdk.llm_eval.eval import BaseBenchmark

    config = ConfigLoader.load_eval_config(config_path)

    if exp_id:
        config.exp_id = exp_id

    return BaseBenchmark(config)


ConfigPathArg = Annotated[str, typer.Argument(help="Path to YAML config file")]
ExpIdOpt = Annotated[str | None, typer.Option("--exp-id", help="Override experiment ID")]


@app.command()
def run(
    config_path: ConfigPathArg,
    exp_id: ExpIdOpt = None,
) -> None:
    """Run full evaluation pipeline: preprocess + judge + stat."""
    benchmark = _load_benchmark(config_path, exp_id)
    asyncio.run(benchmark.main())


@app.command()
def judge(
    config_path: ConfigPathArg,
    exp_id: ExpIdOpt = None,
    stage: Annotated[
        str | None, typer.Option(help="Stage to judge (init/rollout/judged, or none for all)")
    ] = "rollout",
) -> None:
    """Re-judge existing rollout results."""
    benchmark = _load_benchmark(config_path, exp_id)
    _stage = None if stage == "none" else stage
    resolved_stage = cast(Literal["init", "rollout", "judged"] | None, _stage)
    asyncio.run(benchmark.judge(stage=resolved_stage))


@app.command()
def stat(
    config_path: ConfigPathArg,
    exp_id: ExpIdOpt = None,
) -> None:
    """Show evaluation metrics for judged samples."""
    benchmark = _load_benchmark(config_path, exp_id)
    asyncio.run(benchmark.stat())
