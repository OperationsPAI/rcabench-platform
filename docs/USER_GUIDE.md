# User Guide

This guide shows you how to use rcabench-platform both as a console command and as an SDK for developing and evaluating Root Cause Analysis (RCA) algorithms.

## Table of Contents

1. [Installation](#installation)
2. [Console Command Usage](#console-command-usage)
3. [SDK Usage](#sdk-usage)
4. [Common Use Cases](#common-use-cases)
5. [Advanced Topics](#advanced-topics)
6. [Related Documentation](#related-documentation)

## Installation

### Basic Installation

To install rcabench-platform in your project:

```bash
# Using uv (recommended)
uv add rcabench-platform

# Using pip
pip install rcabench-platform
```

### Installation with Analysis Features

For dataset analysis functionality (includes graphviz and matplotlib):

```bash
# Using uv
uv add "rcabench-platform[analysis]"

# Using pip  
pip install "rcabench-platform[analysis]"
```

## Console Command Usage

Once installed, rcabench-platform provides the `rca` command for interacting with the RCABench platform and managing RCA workflows.

### Basic Commands

#### List Available Commands

```bash
rca --help
```

#### Working with Datasets

```bash
# List all available datasets
rca list-datasets

# Get details about a specific dataset
rca get-dataset --id 123

# Search for datasets
rca query-injection --name "your-dataset-name"
```

#### Working with Algorithms

```bash
# List all available algorithms
rca list-algorithms

# Upload a new algorithm from Harbor registry
rca upload-algorithm-harbor /path/to/algorithm/folder

# List injections
rca list-injections
```

#### Algorithm Execution

```bash
# Submit algorithm execution on a dataset
rca submit-execution \
  --algorithm "random" \
  --dataset "my-dataset" \
  --dataset-version "v1.0" \
  --project "my-project" \
  --tag "experiment-1"

# Submit multiple algorithms
rca submit-execution \
  --algorithm "random" \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "my-dataset" \
  --dataset-version "v1.0" \
  --project "my-project" \
  --tag "comparison-experiment"

# Submit execution with custom Docker image
rca submit-execution \
  --algorithm "registry.example.com/my-algo:latest" \
  --dataset "my-dataset" \
  --dataset-version "v1.0" \
  --project "my-project" \
  --tag "custom-algo-test"

# Submit execution with environment variables
rca submit-execution \
  --algorithm "my-algorithm" \
  --dataset "my-dataset" \
  --dataset-version "v1.0" \
  --project "my-project" \
  --tag "env-test" \
  --env "PARAM1=value1" \
  --env "PARAM2=value2"
```

#### Trace Sampling

```bash
# List available trace samplers
python main.py sample show-samplers

# Run a single sampler on a datapack
python main.py sample single \
  --sampler "random" \
  --dataset "my-dataset" \
  --datapack "my-datapack" \
  --sampling-rate 0.1 \
  --mode offline

# Run multiple samplers in batch
python main.py sample batch \
  --sampler "random" \
  --dataset "my-dataset" \
  --rate 0.1 \
  --rate 0.2 \
  --mode offline \
  --mode online

# Generate sampling performance report
python main.py sample perf-report \
  --dataset "my-dataset" \
  --sampler "random" \
  --rate 0.1 \
  --mode offline
```

#### Monitoring and Analysis

```bash
# Trace execution events
rca trace "trace-id-12345" --timeout 600

# Get metrics for a single algorithm
rca metrics \
  --algorithm "random" \
  --dataset "my-dataset" \
  --dataset-version "v1.0" \
  --tag "experiment-1"

# Compare multiple algorithms
rca multi-metrics \
  --algorithm "random" \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "my-dataset" \
  --dataset-version "v1.0" \
  --tag "comparison-experiment"

# Cross-dataset comparison
rca cross-dataset-metrics \
  --algorithm "random" \
  --algorithm "baro" \
  --dataset "dataset1" \
  --dataset "dataset2" \
  --dataset-version "v1.0" \
  --dataset-version "v1.0" \
  --tag "cross-comparison"
```

#### Infrastructure Operations

```bash
# Download Kubernetes cluster information
rca kube-info --namespace "default" --save-path /tmp/kube-info.json
```

### Alternative CLI Entry Point

You can also use the main.py script directly:

```bash
# Clone the repository and run locally
python main.py --help
python main.py online list-datasets
python main.py online submit-execution --help
```

## SDK Usage

The rcabench-platform can be used as a Python SDK for programmatic access to RCA functionality.

### Basic SDK Usage

```python
import rcabench_platform

# Check package version
print(f"rcabench-platform version: {rcabench_platform.__version__}")
```

### Working with RCABench Client

```python
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench.openapi import DatasetsApi, AlgorithmsApi, DtoDatasetV2SearchReq

# Create a client
with RCABenchClient() as client:
    # Work with datasets
    datasets_api = DatasetsApi(client)
    datasets = datasets_api.api_v2_datasets_search_post(
        search=DtoDatasetV2SearchReq(search="")
    )
    print(f"Found {len(datasets.data.items)} datasets")
    
    # Work with algorithms  
    algorithms_api = AlgorithmsApi(client)
    algorithms = algorithms_api.api_v2_algorithms_get()
    print(f"Found {len(algorithms.data.items)} algorithms")
```

### Developing Custom RCA Algorithms

```python
from pathlib import Path
from rcabench_platform.v2.algorithms.spec import Algorithm, AlgorithmArgs, AlgorithmAnswer

class MyCustomAlgorithm(Algorithm):
    def needs_cpu_count(self) -> int | None:
        # Return number of CPU cores needed, or None for all available cores
        return 1
    
    def __call__(self, args: AlgorithmArgs) -> list[AlgorithmAnswer]:
        # Implement your RCA logic here
        # args.dataset: dataset name
        # args.datapack: datapack name  
        # args.input_folder: Path to input data
        # args.output_folder: Path to save results
        
        # Example: Simple random ranking
        services = ["service-a", "service-b", "service-c"]
        results = []
        
        for i, service in enumerate(services):
            results.append(AlgorithmAnswer(
                level="service",
                name=service,
                rank=i + 1
            ))
        
        return results

# Register your algorithm
from rcabench_platform.v2.algorithms.spec import global_algorithm_registry

registry = global_algorithm_registry()
registry["my-custom-algorithm"] = MyCustomAlgorithm
```

### Developing Custom Trace Samplers

```python
from pathlib import Path
from rcabench_platform.v2.samplers.spec import TraceSampler, SamplerArgs, SampleResult, SamplingMode

class MyCustomSampler(TraceSampler):
    def needs_cpu_count(self) -> int | None:
        # Return number of CPU cores needed, or None for all available cores
        return 1
    
    def __call__(self, args: SamplerArgs) -> list[SampleResult]:
        # Implement your sampling logic here
        # args.dataset: dataset name
        # args.datapack: datapack name
        # args.input_folder: Path to input data
        # args.output_folder: Path to save results
        # args.sampling_rate: Target sampling rate (0.0 to 1.0)
        # args.mode: SamplingMode.ONLINE or SamplingMode.OFFLINE
        
        # Load traces from normal_traces.parquet and abnormal_traces.parquet
        import polars as pl
        
        normal_traces_lf = pl.scan_parquet(args.input_folder / "normal_traces.parquet")
        abnormal_traces_lf = pl.scan_parquet(args.input_folder / "abnormal_traces.parquet")
        
        # Get unique trace_ids
        combined_traces_lf = pl.concat([
            normal_traces_lf.select("trace_id"),
            abnormal_traces_lf.select("trace_id")
        ])
        unique_traces = combined_traces_lf.unique().collect()
        trace_ids = unique_traces["trace_id"].to_list()
        
        # Implement your sampling strategy
        results = []
        for trace_id in trace_ids:
            # Calculate sample score based on your algorithm
            sample_score = 0.5  # Example: constant score
            results.append(SampleResult(trace_id=trace_id, sample_score=sample_score))
        
        # Apply sampling mode
        if args.mode == SamplingMode.ONLINE:
            # Online mode: return all traces with scores
            return results
        elif args.mode == SamplingMode.OFFLINE:
            # Offline mode: limit by sampling rate
            results.sort(key=lambda x: x.sample_score, reverse=True)
            target_count = int(len(results) * args.sampling_rate)
            return results[:target_count]
        
        return results

# Register your sampler
from rcabench_platform.v2.samplers.spec import global_sampler_registry

registry = global_sampler_registry()
registry["my-custom-sampler"] = MyCustomSampler
```

### Using Built-in Algorithms

```python
from rcabench_platform.v2.algorithms.random_ import Random
from rcabench_platform.v2.algorithms.rcaeval.baro import Baro
from rcabench_platform.v2.algorithms.spec import AlgorithmArgs
from pathlib import Path

# Use the random algorithm
random_algo = Random()
print(f"Random algorithm needs {random_algo.needs_cpu_count()} CPU cores")

# Create algorithm arguments
args = AlgorithmArgs(
    dataset="my-dataset",
    datapack="my-datapack", 
    input_folder=Path("/path/to/input"),
    output_folder=Path("/path/to/output")
)

# Run the algorithm
results = random_algo(args)
for result in results:
    print(f"Level: {result.level}, Name: {result.name}, Rank: {result.rank}")
```

### Using Built-in Samplers

```python
from rcabench_platform.v2.samplers.random_ import RandomSampler
from rcabench_platform.v2.samplers.spec import SamplerArgs, SamplingMode
from pathlib import Path

# Use the random sampler
random_sampler = RandomSampler(seed=42)  # Optional seed for reproducibility
print(f"Random sampler needs {random_sampler.needs_cpu_count()} CPU cores")

# Create sampler arguments
args = SamplerArgs(
    dataset="my-dataset",
    datapack="my-datapack",
    input_folder=Path("/path/to/input"),
    output_folder=Path("/path/to/output"),
    sampling_rate=0.1,  # 10% sampling rate
    mode=SamplingMode.OFFLINE
)

# Run the sampler
sample_results = random_sampler(args)
for result in sample_results:
    print(f"Trace ID: {result.trace_id}, Score: {result.sample_score}")
```

### Working with Metrics

```python
from rcabench_platform.v2.metrics.algo_metrics import (
    get_metrics_by_dataset,
    get_multi_algorithms_metrics_by_dataset
)

# Get metrics for a single algorithm
metrics = get_metrics_by_dataset(
    algorithm="random",
    dataset="my-dataset", 
    dataset_version="v1.0",
    tag="experiment-1"
)
print(f"Metrics: {metrics}")

# Compare multiple algorithms
comparison_metrics = get_multi_algorithms_metrics_by_dataset(
    algorithms=["random", "baro", "nsigma"],
    dataset="my-dataset",
    dataset_version="v1.0", 
    tag="comparison-experiment"
)
print(f"Comparison metrics: {comparison_metrics}")
```

### Working with Sampler Performance

```python
from rcabench_platform.v2.samplers.experiments import (
    run_sampler_single,
    run_sampler_batch,
    generate_sampler_perf_report
)
from rcabench_platform.v2.samplers.spec import SamplingMode

# Run a single sampler experiment
run_sampler_single(
    sampler="random",
    dataset="my-dataset",
    datapack="my-datapack",
    sampling_rate=0.1,
    mode=SamplingMode.OFFLINE
)

# Run batch sampler experiments
run_sampler_batch(
    samplers=["random"],
    datasets=["my-dataset"],
    sampling_rates=[0.1, 0.2],
    modes=[SamplingMode.OFFLINE, SamplingMode.ONLINE]
)

# Generate performance report
generate_sampler_perf_report(
    datasets=["my-dataset"],
    samplers=["random"],
    sampling_rates=[0.1],
    modes=[SamplingMode.OFFLINE]
)
```

### Working with Configuration

```python
from rcabench_platform.v2.config import get_config

# Get current configuration
config = get_config()
print(f"Temp directory: {config.temp}")
```

### Logging

```python
from rcabench_platform.v2.logging import logger, timeit

# Use structured logging
logger.info("Starting RCA analysis")
logger.error("Analysis failed", error="connection timeout")

# Use timing decorator
@timeit()
def run_analysis():
    # Your analysis code here
    pass

run_analysis()
```

## Common Use Cases

### 1. Running a Quick Algorithm Comparison

```bash
# Compare built-in algorithms on a dataset
rca submit-execution \
  --algorithm "random" \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "test-dataset" \
  --dataset-version "v1.0" \
  --project "algorithm-comparison" \
  --tag "quick-test"

# Wait for completion and get results
rca multi-metrics \
  --algorithm "random" \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "test-dataset" \
  --dataset-version "v1.0" \
  --tag "quick-test"
```

### 2. Developing and Testing a New Algorithm

```python
# 1. Develop your algorithm (see SDK usage above)
# 2. Register it in the global registry
# 3. Test it locally

from rcabench_platform.v2.algorithms.spec import AlgorithmArgs
from pathlib import Path

# Create test data structure
args = AlgorithmArgs(
    dataset="test-dataset",
    datapack="test-datapack",
    input_folder=Path("/tmp/test-input"),
    output_folder=Path("/tmp/test-output")
)

# Test your algorithm
my_algo = MyCustomAlgorithm()
results = my_algo(args)
print(f"Algorithm returned {len(results)} results")
```

### 3. Batch Processing Multiple Datasets

```python
# Process multiple datasets programmatically
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench.openapi import DatasetsApi, DtoDatasetV2SearchReq

datasets_to_process = ["dataset-1", "dataset-2", "dataset-3"]
algorithms_to_test = ["random", "baro"]

with RCABenchClient() as client:
    api = DatasetsApi(client)
    
    for dataset in datasets_to_process:
        for algorithm in algorithms_to_test:
            # Submit execution programmatically
            print(f"Processing {dataset} with {algorithm}")
            # Implementation would go here
```

### 4. Monitoring Long-Running Experiments

```bash
# Submit long-running experiment
rca submit-execution \
  --algorithm "complex-algorithm" \
  --dataset "large-dataset" \
  --dataset-version "v2.0" \
  --project "long-experiment" \
  --tag "production-test"

# Monitor progress (get trace ID from submit response)
rca trace "trace-id-from-submission" --timeout 3600

# Check final results
rca metrics \
  --algorithm "complex-algorithm" \
  --dataset "large-dataset" \
  --dataset-version "v2.0" \
  --tag "production-test"
```

### 5. Trace Sampling Evaluation

```bash
# Run sampling experiments on multiple datasets
python main.py sample batch \
  --sampler "random" \
  --dataset "dataset1" \
  --dataset "dataset2" \
  --rate 0.05 \
  --rate 0.1 \
  --rate 0.2 \
  --mode offline

# Generate comprehensive sampling performance report
python main.py sample perf-report \
  --dataset "dataset1" \
  --dataset "dataset2" \
  --sampler "random"

# Compare sampling strategies
python main.py sample batch \
  --sampler "random" \
  --sampler "my-custom-sampler" \
  --dataset "test-dataset" \
  --rate 0.1 \
  --mode offline \
  --mode online
```

## Advanced Topics

### Custom Docker Images

You can use custom Docker images for algorithms:

```bash
rca submit-execution \
  --algorithm "myregistry.com/my-custom-algo:v1.0" \
  --dataset "my-dataset" \
  --project "custom-algo-test" \
  --tag "docker-test"
```

### Environment Variables

Pass configuration to algorithms via environment variables:

```bash
rca submit-execution \
  --algorithm "configurable-algo" \
  --dataset "my-dataset" \
  --project "config-test" \
  --tag "env-test" \
  --env "THRESHOLD=0.95" \
  --env "MAX_ITERATIONS=100"
```

### Algorithm Upload Process

For algorithms stored in Harbor registry:

1. Create algorithm folder with required files:
   - `info.toml`: Algorithm metadata
   - `Dockerfile`: Container definition  
   - `entrypoint.sh`: Algorithm entry point

2. Upload to platform:
```bash
rca upload-algorithm-harbor /path/to/algorithm/folder
```

### Working with Service Dependency Graphs

The platform includes SDG (Service Dependency Graph) functionality accessible via:

```bash
python main.py sdg --help
```

### Trace Sampling Features

The platform provides comprehensive trace sampling capabilities for evaluating sampling algorithms:

#### Sampling Modes

- **Online Mode**: Returns all traces with their sampling scores, no limit on count
- **Offline Mode**: Limited by sampling rate, sorts traces by score and keeps top traces

#### Performance Metrics

The sampling framework calculates the following performance metrics:

- **Controllability (RoD)**: Rate of Deviation - measures sampling rate accuracy
- **Comprehensiveness (CR)**: Coverage Rate - measures trace type diversity  
- **Proportion Metrics (PRO)**: Three types of proportion analysis:
  - `proportion_anomaly`: Proportion of detector-flagged spans in abnormal traces only
  - `proportion_rare`: Proportion of rare entry spans sampled (< 5% frequency)
  - `proportion_common`: Proportion of common spans (including detector spans in normal traces)
- **Runtime Performance**: Runtime per span in milliseconds
- **Actual Sampling Rate**: Achieved vs. target sampling rate

#### Output Structure

Sampling results are stored in:
```
{output}/sampled/{dataset}/{datapack}/{sampler}_{sampling_rate}_{mode}/
├── online.parquet or offline.parquet  # Sampling results
├── perf.parquet                        # Performance metrics
└── .finished                          # Completion marker
```

## Related Documentation

For more detailed information, refer to:

- [Development Guide](../CONTRIBUTING.md): Setting up development environment
- [Installation Guide](./INSTALL.md): Detailed installation instructions  
- [Specifications](./specifications.md): Technical specifications and data formats
- [Workflow References](./workflow-references.md): Detailed workflow documentation
- [Maintenance Guide](./maintenance.md): Project maintenance and release procedures

## Support

For issues and questions:
- Check the [GitHub Issues](https://github.com/LGU-SE-Internal/rcabench-platform/issues)
- Review the existing documentation in the `docs/` directory
- See related projects: [rcabench](https://github.com/LGU-SE-Internal/rcabench), [rca-algo-contrib](https://github.com/LGU-SE-Internal/rca-algo-contrib)