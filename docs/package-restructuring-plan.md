# Package Restructuring Plan

## Status: Implemented ✅

This document describes the restructuring of `rcabench-platform` from a monolithic package into a modular package with clear audience-based directory separation and optional dependency groups.

## Background

The `rcabench-platform` package previously had all 95+ Python files in a flat directory structure under `v2/`, with 30+ mandatory dependencies. This caused:

- **Bloated installs**: Algorithm developers pulled in ~30+ dependencies they never use
- **Windows incompatibility**: `cli/sdg.py` imported the Unix-only `resource` module at the top level (see issue #76)
- **Unclear boundaries**: SDK interfaces, platform internals, and analysis tools were all mixed in the same directory

## Solution

### 1. Code Directory Restructuring

The codebase is now organized into three audience-based subpackages under `v2/`:

```
v2/
├── sdk/                    # Core SDK for algorithm/sampler developers
│   ├── algorithms/         # RCA algorithm base classes & implementations
│   ├── samplers/           # Trace sampler base classes & implementations
│   ├── datasets/           # Dataset/datapack specs & helpers
│   ├── evaluation/         # Performance metrics (Avg@k, Top-k, MRR, etc.)
│   ├── experiments/        # Experiment runners (single, batch, report)
│   ├── graphs/             # SDG data structures & builders
│   ├── pedestals/          # Dataset-specific processors
│   ├── utils/              # Shared utilities (serde, fmap, fs, etc.)
│   ├── config.py           # Environment configuration
│   └── logging.py          # Structured logging with loguru
│
├── internal/               # Platform-internal modules (operators)
│   ├── clients/            # Infrastructure clients (rcabench, k8s, neo4j, clickhouse)
│   ├── cloud/              # Cloud storage (MinIO, HuggingFace Hub)
│   ├── sources/            # Dataset format converters
│   └── metrics/            # Algorithm metrics & anomaly detection
│
├── analysis/               # Research & visualization tools
│   ├── aggregation.py      # Fault type mapping
│   ├── algo_perf_vis.py    # Algorithm performance visualization
│   ├── data_prepare.py     # Data preparation
│   └── ...
│
├── tools/                  # Interactive tools
│   └── label/              # Streamlit-based trace labeling UI
│
├── cli/                    # CLI entry points
│   ├── main.py             # Main entry (loads eval, sample, tools always)
│   ├── eval.py             # Algorithm evaluation commands
│   ├── sample.py           # Sampler execution commands
│   ├── tools.py            # Utility commands
│   ├── online.py           # Server API commands (requires [internal])
│   ├── sdg.py              # SDG builder commands (requires [internal])
│   ├── container.py        # Container management (requires [internal])
│   └── self_.py            # Self-test commands (requires [internal])
│
├── config.py               # Backward-compatible re-export from sdk/
└── logging.py              # Backward-compatible re-export from sdk/
```

### 2. Optional Dependency Groups

**Base install: `rcabench-platform`** (SDK for algorithm/sampler developers)
```bash
pip install rcabench-platform
```
Core dependencies (~15 packages): `polars`, `pyarrow`, `numpy`, `scipy`, `scikit-learn`, `duckdb`, `networkx`, `loguru`, `typer`, etc.

**`rcabench-platform[internal]`** (Platform maintainers)
```bash
pip install "rcabench-platform[internal]"
```
Additional: `rcabench`, `kubernetes`, `neo4j`, `clickhouse-connect`, `minio`, `huggingface-hub`, `drain3`

**`rcabench-platform[analysis]`** (Researchers)
```bash
pip install "rcabench-platform[analysis]"
```
Additional: `matplotlib`, `plotly`, `altair`, `streamlit`, `statsmodels`, `graphviz`, etc.

**`rcabench-platform[all]`** (Full installation)
```bash
pip install "rcabench-platform[all]"
```

### 3. Import Path Changes

| Old Path | New Path |
|----------|----------|
| `v2.algorithms.spec` | `v2.sdk.algorithms.spec` |
| `v2.samplers.spec` | `v2.sdk.samplers.spec` |
| `v2.datasets.spec` | `v2.sdk.datasets.spec` |
| `v2.evaluation.ranking` | `v2.sdk.evaluation.ranking` |
| `v2.experiments.single` | `v2.sdk.experiments.single` |
| `v2.config` | `v2.sdk.config` (also `v2.config` via re-export) |
| `v2.logging` | `v2.sdk.logging` (also `v2.logging` via re-export) |
| `v2.utils.*` | `v2.sdk.utils.*` |
| `v2.clients.*` | `v2.internal.clients.*` |
| `v2.cloud.*` | `v2.internal.cloud.*` |
| `v2.sources.*` | `v2.internal.sources.*` |
| `v2.metrics.*` | `v2.internal.metrics.*` |

**Backward compatibility**: `v2.config` and `v2.logging` remain usable via re-export modules.

## Implementation Details

### Conditional CLI Loading

```python
def main(*, enable_builtin_algorithms: bool = True) -> None:
    from . import eval, sample, tools
    # Always available
    app.add_typer(tools.app, name="tools")
    app.add_typer(eval.app, name="eval")
    app.add_typer(sample.app, name="sample")

    # Requires rcabench-platform[internal]
    try:
        from . import container, online, sdg, self_
        app.add_typer(self_.app, name="self")
        app.add_typer(online.app, name="online")
        app.add_typer(sdg.app, name="sdg")
        app.add_typer(container.app, name="container")
    except ImportError:
        pass
```

### Windows Compatibility

The `resource` module import in `cli/sdg.py` is guarded with `sys.platform != "win32"` (fixes #76).

## Migration Guide

| Previous Install | New Install |
|-----------------|-------------|
| `pip install rcabench-platform` | `pip install "rcabench-platform[all]"` (to keep same behavior) |
| Algorithm developer | `pip install rcabench-platform` (lightweight SDK) |
| Platform operator | `pip install "rcabench-platform[internal]"` |
| Researcher | `pip install "rcabench-platform[analysis]"` |

## Related Issues

- #76 — Windows support blocked by `resource` module in `sdg.py` (fixed)
- #80 — LLM agent evaluation judge needs clean SDK interface
