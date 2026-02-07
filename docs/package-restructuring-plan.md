# Package Restructuring Plan

## Background

The current `rcabench-platform` monolithic package mixes three distinct use cases:

1. **Algorithm/Sampler SDK** — External developers who only need to implement and evaluate RCA algorithms or trace samplers
2. **Platform Internal Services** — Internal tools for data collection, infrastructure management, and server-side operations
3. **Data Pipeline & Analysis** — Dataset conversion, visualization, and research analysis tools

This causes problems:
- Algorithm developers must install heavy dependencies they don't need (`kubernetes`, `neo4j`, `clickhouse-connect`, `minio`, etc.)
- Platform-internal modules (SDG builder, container management, online API) are exposed to SDK users unnecessarily
- Windows users cannot use the SDK at all because `cli/sdg.py` imports the Unix-only `resource` module (see issue #76)
- The dependency list is large and monolithic, increasing install time, compatibility issues and attack surface

## Proposed Package Structure

### Package 1: `rcabench-sdk` (For Algorithm & Sampler Developers)

**Purpose**: Lightweight SDK for developing, running, and evaluating RCA algorithms and trace samplers.

**Target users**: Algorithm researchers, sampler developers, external contributors.

**Modules to include**:

| Module | Description |
|--------|-------------|
| `algorithms/spec.py` | `Algorithm` base class, `AlgorithmArgs`, `AlgorithmAnswer`, `AlgorithmRegistry` |
| `algorithms/random_.py` | Reference algorithm implementation |
| `samplers/spec.py` | `TraceSampler` base class, `SamplerArgs`, `SampleResult`, registry |
| `samplers/random_.py` | Reference sampler implementation |
| `samplers/registry.py` | Sampler registration helpers |
| `samplers/metrics_sli.py` | SLI metrics generation |
| `samplers/event_encoding.py` | Event encoding for sampler metrics |
| `samplers/path_encoding.py` | Path encoding for sampler metrics |
| `datasets/spec.py` | Dataset/datapack path helpers, data loaders, `DatasetAnalyzer` base class |
| `evaluation/ranking.py` | Performance metrics (Avg@k, Top-k, MRR, MAP, etc.) |
| `experiments/spec.py` | Output folder helpers |
| `experiments/single.py` | Single algorithm/datapack runner |
| `experiments/batch.py` | Batch experiment runner |
| `experiments/report.py` | Performance report generator |
| `samplers/experiments/` | Sampler experiment runners (single, batch, report) |
| `config.py` | Environment configuration (data/output paths) |
| `logging.py` | Structured logging with loguru |
| `utils/` | All utility modules (dataframe, env, fs, serde, fmap, display, dict_, profiler) |
| `cli/main.py` | CLI entry point (eval + sample subcommands only) |
| `cli/eval.py` | Algorithm evaluation commands |
| `cli/sample.py` | Sampler execution commands |
| `cli/tools.py` | Basic utility commands (parquet-head) |
| `cli/self_.py` | Self-test commands |

**Dependencies** (minimal):
- `polars`, `pyarrow` — Data processing
- `numpy`, `scipy`, `scikit-learn` — Statistical computation
- `pandas` — DataFrame compatibility
- `loguru`, `tqdm`, `typer` — Logging, progress, CLI
- `python-dotenv`, `tomli` — Configuration
- `pydantic` — Data validation

**Entry points**:
- `rca` CLI command (eval + sample subcommands)

---

### Package 2: `rcabench-platform-internal` (For Platform Maintainers)

**Purpose**: Internal platform tools for infrastructure management, data collection, server integration, and service graph analysis.

**Target users**: Platform maintainers, DevOps, data pipeline operators.

**Modules to include**:

| Module | Description |
|--------|-------------|
| `clients/rcabench_.py` | RCABench server API client |
| `clients/k8s.py` | Kubernetes cluster info |
| `clients/neo4j.py` | Neo4j graph database client |
| `clients/clickhouse.py` | ClickHouse metrics database client |
| `cloud/spec.py` | Abstract storage interface |
| `cloud/hf.py` | HuggingFace Hub integration |
| `cloud/minio_.py` | MinIO object storage |
| `graphs/sdg/` | Service Dependency Graph builder |
| `graphs/networkx/` | NetworkX graph utilities |
| `pedestals/` | Dataset-specific processors (Train-Ticket) |
| `sources/` | Dataset format converters (RCABench, RCAEval) |
| `cli/online.py` | Server API integration commands |
| `cli/sdg.py` | SDG builder commands |
| `cli/container.py` | Container management commands |
| `tools/label/` | Streamlit labeling UI |

**Additional dependencies** (beyond `rcabench-sdk`):
- `rcabench` — OpenAPI client for RCABench server
- `kubernetes` — K8s cluster interaction
- `neo4j` — Graph database
- `clickhouse-connect` — ClickHouse DB
- `minio` — Object storage
- `huggingface-hub` — Model/dataset hub
- `drain3` — Log template extraction
- `networkx` — Graph algorithms
- `resource` (stdlib, Unix-only) — System resource usage monitoring

**Entry points**:
- `rca` CLI command (all subcommands including online, sdg, container)
- `label` Streamlit app

---

### Package 3: `rcabench-analysis` (For Research & Visualization)

**Purpose**: Data analysis, visualization, and research tools for exploring RCA experiment results.

**Target users**: Data scientists, researchers analyzing experiment results.

**Modules to include**:

| Module | Description |
|--------|-------------|
| `analysis/aggregation.py` | Fault type mapping & result aggregation |
| `analysis/algo_perf_vis.py` | Algorithm performance visualization |
| `analysis/data_prepare.py` | Data preparation utilities |
| `analysis/datapacks_analysis.py` | Datapack-level analysis |
| `analysis/datapacks_visualization.py` | Datapack visualization |
| `analysis/detector_visualization.py` | Detection result visualization |
| `metrics/algo_metrics.py` | Algorithm metrics aggregation |
| `metrics/metrics_calculator.py` | Metrics computation |
| `metrics/ad/` | Anomaly detection modules |
| `datasets/rcabench.py` | RCABench dataset analyzer |
| `datasets/rcaeval.py` | RCAEval dataset analyzer |

**Additional dependencies** (beyond `rcabench-sdk`):
- `matplotlib`, `plotly`, `altair` — Visualization
- `kaleido` — Static image export
- `vega-datasets`, `vegafusion` — Vega chart support
- `graphviz` — Graph visualization
- `streamlit` (optional) — Interactive dashboards
- `duckdb` — Analytical queries
- `statsmodels` — Statistical analysis

---

## Dependency Graph

```
rcabench-analysis
    └── depends on: rcabench-sdk

rcabench-platform-internal
    └── depends on: rcabench-sdk

rcabench-sdk (standalone, minimal dependencies)
```

## Migration Strategy

### Phase 1: Internal Restructuring (Non-breaking)
1. Reorganize modules within the current package into clear subdirectories reflecting the three packages
2. Add `__init__.py` files to define clean public APIs for each logical package
3. Ensure all cross-package imports go through the public API boundaries

### Phase 2: Extract `rcabench-sdk`
1. Create a new `rcabench-sdk` package with the SDK modules
2. Update `rcabench-platform` to depend on `rcabench-sdk`
3. Add re-exports in `rcabench-platform` for backward compatibility
4. Update documentation and user guides

### Phase 3: Extract `rcabench-platform-internal`
1. Create `rcabench-platform-internal` with platform-specific modules
2. Move heavy dependencies to this package
3. The original `rcabench-platform` becomes a meta-package that depends on all three

### Phase 4: Extract `rcabench-analysis`
1. Create `rcabench-analysis` with visualization/analysis modules
2. Move visualization dependencies to this package
3. Update notebooks and analysis scripts

### Phase 5: Cleanup
1. Remove backward-compatibility re-exports after deprecation period
2. Update CI/CD pipelines
3. Publish all packages to PyPI
4. Update downstream projects (rca-algo-contrib, rca-algo-random, RCAgentEval)

## Benefits

| Benefit | Description |
|---------|-------------|
| **Lighter installs** | Algorithm developers only install `rcabench-sdk` (~10 dependencies vs ~30+) |
| **Windows support** | `rcabench-sdk` has no Unix-only dependencies (fixes #76) |
| **Clearer boundaries** | Each package has a well-defined purpose and audience |
| **Faster CI** | SDK tests don't need infrastructure services |
| **Better security** | Smaller dependency surface per package |
| **Independent releases** | SDK can be versioned separately from platform internals |

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Breaking existing imports | Use re-exports and deprecation warnings during migration |
| Circular dependencies | Phase 1 internal restructuring catches these early |
| Version synchronization | Use compatible version ranges between packages |
| Increased maintenance | Offset by clearer ownership and smaller scope per package |

## Related Issues

- #76 — Windows support blocked by `resource` module in `sdg.py`
- #80 — LLM agent evaluation judge needs clean SDK interface
