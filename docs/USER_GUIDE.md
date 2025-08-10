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