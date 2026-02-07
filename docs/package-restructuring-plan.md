# Package Restructuring Plan

## Status: Implemented ✅

This document describes the restructuring of `rcabench-platform` from a monolithic package with 30+ mandatory dependencies into a modular package using optional dependency groups.

## Background

The `rcabench-platform` package previously required all dependencies upfront, including heavy infrastructure clients (`kubernetes`, `neo4j`, `clickhouse-connect`, `minio`, etc.) that algorithm developers never need. This caused:

- **Bloated installs**: Algorithm developers pulled in ~30+ dependencies they never use
- **Windows incompatibility**: `cli/sdg.py` imported the Unix-only `resource` module at the top level (see issue #76)
- **Unclear boundaries**: SDK interfaces, platform internals, and analysis tools were all mixed together

## Solution: Optional Dependency Groups

The package now uses three installation profiles via optional dependency groups:

### Base Install: `rcabench-platform` (SDK for Algorithm & Sampler Developers)

```bash
uv add rcabench-platform
# or
pip install rcabench-platform
```

**Target users**: Algorithm researchers, sampler developers, external contributors.

**Included modules**:

| Module | Description |
|--------|-------------|
| `algorithms/spec.py` | `Algorithm` base class, `AlgorithmArgs`, `AlgorithmAnswer`, `AlgorithmRegistry` |
| `algorithms/random_.py` | Reference algorithm implementation |
| `algorithms/traceback/` | TraceBACK algorithm family (A7–A10) |
| `algorithms/rcaeval/` | RCAEval baseline algorithms (baro, nsigma) |
| `samplers/spec.py` | `TraceSampler` base class, `SamplerArgs`, `SampleResult`, registry |
| `samplers/random_.py` | Reference sampler implementation |
| `samplers/experiments/` | Sampler experiment runners (single, batch, report) |
| `datasets/spec.py` | Dataset/datapack path helpers, data loaders, `DatasetAnalyzer` base class |
| `evaluation/ranking.py` | Performance metrics (Avg@k, Top-k, MRR, MAP, etc.) |
| `experiments/` | Experiment runners (single, batch, report) |
| `config.py` | Environment configuration (data/output paths) |
| `logging.py` | Structured logging with loguru |
| `utils/` | All utility modules (dataframe, env, fs, serde, fmap, display, dict_, profiler) |
| `cli/main.py` | CLI entry point (`eval` + `sample` + `tools` subcommands) |

**Core dependencies** (~15 packages):
- `polars`, `pyarrow`, `pandas` — Data processing
- `numpy`, `scipy`, `scikit-learn` — Statistical computation
- `duckdb`, `networkx` — Query and graph processing
- `loguru`, `tqdm`, `typer` — Logging, progress, CLI
- `python-dotenv`, `tomli`, `pydantic`, `requests` — Configuration and utilities

---

### `rcabench-platform[internal]` (For Platform Maintainers)

```bash
uv add "rcabench-platform[internal]"
# or
pip install "rcabench-platform[internal]"
```

**Target users**: Platform maintainers, DevOps, data pipeline operators.

**Additional modules enabled**:

| Module | Description |
|--------|-------------|
| `clients/rcabench_.py` | RCABench server API client |
| `clients/k8s.py` | Kubernetes cluster info |
| `clients/neo4j.py` | Neo4j graph database client |
| `clients/clickhouse.py` | ClickHouse metrics database client |
| `cloud/hf.py` | HuggingFace Hub integration |
| `cloud/minio_.py` | MinIO object storage |
| `graphs/sdg/` | Service Dependency Graph builder |
| `sources/` | Dataset format converters (RCABench, RCAEval) |
| `cli/online.py` | Server API integration commands |
| `cli/sdg.py` | SDG builder commands |
| `cli/container.py` | Container management commands |
| `cli/self_.py` | Self-test commands (ping ClickHouse, RCABench) |

**Additional dependencies**:
- `rcabench` — OpenAPI client for RCABench server
- `kubernetes` — K8s cluster interaction
- `neo4j` — Graph database
- `clickhouse-connect` — ClickHouse DB
- `minio` — Object storage
- `huggingface-hub` — Model/dataset hub
- `drain3` — Log template extraction

---

### `rcabench-platform[analysis]` (For Research & Visualization)

```bash
uv add "rcabench-platform[analysis]"
# or
pip install "rcabench-platform[analysis]"
```

**Target users**: Data scientists, researchers analyzing experiment results.

**Additional modules enabled**:

| Module | Description |
|--------|-------------|
| `analysis/` | Fault aggregation, algorithm performance visualization, datapack analysis |
| `metrics/` | Algorithm metrics aggregation, anomaly detection |
| `tools/label/` | Streamlit-based trace labeling UI |

**Additional dependencies**:
- `matplotlib`, `plotly`, `altair` — Visualization
- `kaleido` — Static image export
- `vega-datasets`, `vegafusion` — Vega chart support
- `graphviz` — Graph visualization
- `statsmodels` — Statistical analysis
- `streamlit` — Interactive dashboards

---

### `rcabench-platform[all]` (Full Installation)

```bash
uv add "rcabench-platform[all]"
# or
pip install "rcabench-platform[all]"
```

Installs all optional dependencies (equivalent to the old monolithic install).

---

## Implementation Details

### Conditional CLI Loading

The CLI entry point (`cli/main.py`) now loads internal modules conditionally:

```python
def main(*, enable_builtin_algorithms: bool = True) -> None:
    from . import eval, sample, tools

    app.add_typer(tools.app, name="tools")
    app.add_typer(eval.app, name="eval")
    app.add_typer(sample.app, name="sample")

    # Platform-internal CLI modules require optional dependencies
    try:
        from . import container, online, sdg, self_
        app.add_typer(self_.app, name="self")
        app.add_typer(online.app, name="online")
        app.add_typer(sdg.app, name="sdg")
        app.add_typer(container.app, name="container")
    except ImportError:
        pass
```

### Windows Compatibility Fix

The `resource` module import in `cli/sdg.py` is now guarded:

```python
if sys.platform != "win32":
    import resource
    maxrss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    ...
```

### Entry Point Change

The `rca` CLI entry point now uses `cli/main.py:main` (the full modular CLI) instead of `cli/online.py:main` (which required `rcabench` package).

## Benefits

| Benefit | Description |
|---------|-------------|
| **Lighter installs** | Algorithm developers install ~15 deps vs ~30+ |
| **Windows support** | Base SDK has no Unix-only dependencies (fixes #76) |
| **Clearer boundaries** | Each optional group has a well-defined purpose and audience |
| **Faster CI** | SDK-only tests don't need infrastructure services |
| **Better security** | Smaller mandatory dependency surface |
| **Backward compatible** | `pip install rcabench-platform[all]` still installs everything |

## Migration Guide for Existing Users

| Previous Install | New Install | Notes |
|-----------------|-------------|-------|
| `pip install rcabench-platform` | `pip install "rcabench-platform[all]"` | To keep the same behavior as before |
| Algorithm developer | `pip install rcabench-platform` | Lightweight SDK only |
| Platform operator | `pip install "rcabench-platform[internal]"` | Server/infra tools |
| Researcher | `pip install "rcabench-platform[analysis]"` | Visualization tools |
| Full development | `pip install "rcabench-platform[all]"` | Everything |

## Related Issues

- #76 — Windows support blocked by `resource` module in `sdg.py` (fixed)
- #80 — LLM agent evaluation judge needs clean SDK interface
