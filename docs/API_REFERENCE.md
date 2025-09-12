# API Reference

## Overview

rcabench-platform provides both a command-line interface (CLI) and a Python SDK for developing and executing Root Cause Analysis algorithms. This document provides comprehensive API reference for both interfaces.

## Command Line Interface (CLI)

### Installation and Setup

After installing rcabench-platform, the `rca` command becomes available:

```bash
pip install rcabench-platform
rca --help
```

### Global Options

All CLI commands support these global options:

```bash
rca [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS]

Global Options:
  --env-mode TEXT     Environment mode (debug, dev, prod)
  --config PATH       Path to configuration file
  --verbose          Enable verbose logging
  --help             Show help message
```

### Dataset Commands

#### `rca list-datasets`

List all available datasets.

```bash
rca list-datasets [OPTIONS]

Options:
  --format TEXT      Output format (table, json, csv)
  --filter TEXT      Filter datasets by pattern
  --show-stats       Show dataset statistics
```

**Example:**
```bash
rca list-datasets --format json --show-stats
```

**Output:**
```json
[
  {
    "name": "rcabench",
    "version": "v1.0",
    "datapacks": 1250,
    "size_gb": 45.2,
    "created": "2024-01-15T10:30:00Z"
  }
]
```

#### `rca dataset-info`

Get detailed information about a specific dataset.

```bash
rca dataset-info DATASET_NAME [OPTIONS]

Arguments:
  DATASET_NAME       Name of the dataset

Options:
  --version TEXT     Dataset version (default: latest)
  --show-samples     Show sample datapack names
  --export PATH      Export info to file
```

**Example:**
```bash
rca dataset-info rcabench --version v1.0 --show-samples
```

### Algorithm Commands

#### `rca list-algorithms`

List all available algorithms.

```bash
rca list-algorithms [OPTIONS]

Options:
  --type TEXT        Algorithm type (builtin, custom, docker)
  --format TEXT      Output format (table, json, csv)
  --show-details     Show algorithm details
```

**Example:**
```bash
rca list-algorithms --type builtin --show-details
```

#### `rca submit-execution`

Submit algorithm execution for evaluation.

```bash
rca submit-execution [OPTIONS]

Options:
  --algorithm TEXT       Algorithm name or Docker image (multiple allowed)
  --dataset TEXT         Dataset name (multiple allowed)
  --dataset-version TEXT Dataset version
  --project TEXT         Project name for grouping
  --tag TEXT            Execution tag for identification
  --env TEXT            Environment variables (KEY=VALUE, multiple allowed)
  --timeout INTEGER     Execution timeout in seconds
  --dry-run             Validate without executing
```

**Example:**
```bash
rca submit-execution \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "rcabench" \
  --dataset-version "v1.0" \
  --project "comparison-study" \
  --tag "experiment-1" \
  --env "THRESHOLD=0.95" \
  --timeout 3600
```

### Analysis Commands

#### `rca multi-metrics`

Compare metrics across multiple algorithm executions.

```bash
rca multi-metrics [OPTIONS]

Options:
  --algorithm TEXT   Algorithm names to compare (multiple allowed)
  --dataset TEXT     Dataset name
  --tag TEXT         Execution tag
  --metric TEXT      Specific metrics to show (precision, recall, f1)
  --format TEXT      Output format (table, json, csv)
  --export PATH      Export results to file
```

**Example:**
```bash
rca multi-metrics \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "rcabench" \
  --tag "experiment-1" \
  --format table
```

#### `rca execution-status`

Check status of algorithm executions.

```bash
rca execution-status [OPTIONS]

Options:
  --execution-id TEXT  Specific execution ID
  --project TEXT       Project name filter
  --tag TEXT          Tag filter
  --status TEXT       Status filter (running, completed, failed)
  --limit INTEGER     Maximum number of results
```

### Infrastructure Commands

#### `rca kube-info`

Download Kubernetes cluster information.

```bash
rca kube-info [OPTIONS]

Options:
  --namespace TEXT     Kubernetes namespace
  --save-path PATH     Path to save cluster info
  --format TEXT        Output format (json, yaml)
```

**Example:**
```bash
rca kube-info --namespace "rcabench" --save-path /tmp/cluster-info.json
```

#### `rca upload-algorithm-harbor`

Upload algorithm to Harbor registry.

```bash
rca upload-algorithm-harbor ALGORITHM_PATH [OPTIONS]

Arguments:
  ALGORITHM_PATH     Path to algorithm folder

Options:
  --registry TEXT    Harbor registry URL
  --project TEXT     Harbor project name
  --tag TEXT         Algorithm version tag
  --push             Push image after building
```

**Example:**
```bash
rca upload-algorithm-harbor /path/to/my-algorithm \
  --registry "harbor.example.com" \
  --project "algorithms" \
  --tag "v1.0" \
  --push
```

## Python SDK

### Core Modules

#### rcabench_platform.v2.algorithms

Algorithm development and execution framework.

```python
from rcabench_platform.v2.algorithms.spec import AlgorithmArgs, AlgorithmResult
from rcabench_platform.v2.algorithms.spec import global_algorithm_registry

# Algorithm interface
class MyAlgorithm:
    def __call__(self, args: AlgorithmArgs) -> List[AlgorithmResult]:
        # Algorithm implementation
        pass

# Register algorithm
registry = global_algorithm_registry()
registry["my-algorithm"] = MyAlgorithm
```

##### AlgorithmArgs

Data class containing algorithm execution parameters.

```python
@dataclass
class AlgorithmArgs:
    dataset: str           # Dataset name
    datapack: str          # Datapack name
    input_folder: Path     # Input data folder
    output_folder: Path    # Output results folder
```

##### AlgorithmResult

Data class for algorithm results.

```python
@dataclass
class AlgorithmResult:
    service_name: str      # Identified service
    score: float          # Anomaly score (0.0 to 1.0)
    reason: str           # Explanation for the result
    metadata: dict = None # Additional metadata
    
    def to_dict(self) -> dict:
        """Convert to dictionary format"""
        pass
```

#### rcabench_platform.v2.datasets

Dataset management and access utilities.

```python
from rcabench_platform.v2.datasets import DatasetManager, load_dataset_index

# Dataset management
config = get_config()
dm = DatasetManager(config)

# Load dataset information
dataset_info = dm.get_dataset_info("rcabench")
datapack = dm.load_datapack("rcabench", "datapack-001")

# Direct index access
index = load_dataset_index("rcabench")
```

##### DatasetManager

Main class for dataset operations.

```python
class DatasetManager:
    def __init__(self, config: Config):
        """Initialize with configuration"""
        pass
    
    def get_dataset_info(self, dataset_name: str) -> DatasetInfo:
        """Get dataset metadata"""
        pass
    
    def load_datapack(self, dataset_name: str, datapack_name: str) -> Datapack:
        """Load specific datapack"""
        pass
    
    def list_datasets(self) -> List[str]:
        """List available datasets"""
        pass
```

##### Datapack

Data container for telemetry data.

```python
@dataclass
class Datapack:
    name: str
    traces: pd.DataFrame    # Trace data
    metrics: pd.DataFrame   # Metrics data
    logs: pd.DataFrame      # Log data
    metadata: dict          # Additional metadata
    
    def get_services(self) -> List[str]:
        """Get list of services in datapack"""
        pass
    
    def get_time_range(self) -> Tuple[datetime, datetime]:
        """Get time range of the data"""
        pass
```

#### rcabench_platform.v2.config

Configuration management.

```python
from rcabench_platform.v2.config import get_config, set_config, Config

# Get current configuration
config = get_config()
print(config.data)  # Data directory
print(config.output)  # Output directory

# Set custom configuration
custom_config = Config(
    env_mode="debug",
    data=Path("/custom/data"),
    output=Path("/custom/output"),
    temp=Path("/tmp"),
    base_url="http://localhost:8082"
)
set_config(custom_config)
```

##### Config

Configuration data class.

```python
@dataclass
class Config:
    env_mode: str    # Environment mode (debug, dev, prod)
    data: Path       # Dataset storage path
    output: Path     # Results output path
    temp: Path       # Temporary files path
    base_url: str    # Service base URL
```

#### rcabench_platform.v2.execution

Algorithm execution engine.

```python
from rcabench_platform.v2.execution import ExecutionEngine, ExecutionResult

# Create execution engine
engine = ExecutionEngine(config)

# Submit execution
execution_id = await engine.submit_execution(
    algorithm="baro",
    dataset="rcabench",
    project="my-project",
    tag="experiment"
)

# Wait for completion
result = await engine.wait_for_completion(execution_id)
```

##### ExecutionEngine

Main execution orchestrator.

```python
class ExecutionEngine:
    def __init__(self, config: Config):
        """Initialize with configuration"""
        pass
    
    async def submit_execution(
        self,
        algorithm: str,
        dataset: str,
        project: str = None,
        tag: str = None,
        **kwargs
    ) -> str:
        """Submit algorithm execution"""
        pass
    
    async def wait_for_completion(self, execution_id: str) -> ExecutionResult:
        """Wait for execution to complete"""
        pass
    
    async def get_execution_status(self, execution_id: str) -> ExecutionStatus:
        """Get current execution status"""
        pass
```

### Data Structures

#### Telemetry Data Formats

**Traces DataFrame**:
```python
# Column specification for traces.parquet
columns = {
    'trace_id': 'string',        # Unique trace identifier
    'span_id': 'string',         # Unique span identifier
    'parent_span_id': 'string',  # Parent span identifier
    'service_name': 'string',    # Service that generated the span
    'operation_name': 'string',  # Operation being performed
    'start_time': 'datetime64[ns]',  # Span start time (UTC)
    'end_time': 'datetime64[ns]',    # Span end time (UTC)
    'duration_ms': 'float64',    # Duration in milliseconds
    'status_code': 'int64',      # HTTP status code
    'tags.*': 'string'           # Additional span tags
}
```

**Metrics DataFrame**:
```python
# Column specification for metrics.parquet
columns = {
    'time': 'datetime64[ns]',    # Metric timestamp (UTC)
    'metric': 'string',          # Metric name
    'value': 'float64',          # Metric value
    'service_name': 'string',    # Service that generated the metric
    'instance': 'string',        # Service instance identifier
    'labels.*': 'string'         # Additional metric labels
}
```

**Logs DataFrame**:
```python
# Column specification for logs.parquet
columns = {
    'time': 'datetime64[ns]',    # Log timestamp (UTC)
    'trace_id': 'string',        # Associated trace identifier
    'span_id': 'string',         # Associated span identifier
    'service_name': 'string',    # Service that generated the log
    'level': 'string',           # Log level (INFO, ERROR, etc.)
    'message': 'string',         # Log message
    'attributes.*': 'string'     # Additional log attributes
}
```

### Utility Functions

#### Data Loading Utilities

```python
from rcabench_platform.v2.utils import load_parquet, save_parquet, validate_datapack

# Load data efficiently
traces = load_parquet("/path/to/traces.parquet")
metrics = load_parquet("/path/to/metrics.parquet")

# Save results
save_parquet(results_df, "/path/to/results.parquet")

# Validate datapack structure
is_valid, errors = validate_datapack("/path/to/datapack")
```

#### Visualization Utilities

```python
from rcabench_platform.v2.visualization import plot_service_graph, plot_metrics_timeline

# Visualize service dependency graph
graph = plot_service_graph(traces_df)
graph.show()

# Plot metrics over time
timeline = plot_metrics_timeline(metrics_df, service="web-service")
timeline.show()
```

### Error Handling

#### Common Exceptions

```python
from rcabench_platform.v2.exceptions import (
    DatasetNotFoundError,
    AlgorithmNotFoundError,
    ExecutionError,
    ValidationError
)

try:
    datapack = dm.load_datapack("nonexistent-dataset", "datapack-001")
except DatasetNotFoundError as e:
    print(f"Dataset not found: {e}")

try:
    algorithm = registry["nonexistent-algorithm"]
except AlgorithmNotFoundError as e:
    print(f"Algorithm not found: {e}")
```

### Advanced Usage

#### Custom Data Sources

```python
from rcabench_platform.v2.datasets import DataSource

class CustomDataSource(DataSource):
    def load_traces(self, datapack_name: str) -> pd.DataFrame:
        # Custom trace loading logic
        pass
    
    def load_metrics(self, datapack_name: str) -> pd.DataFrame:
        # Custom metrics loading logic
        pass
    
    def load_logs(self, datapack_name: str) -> pd.DataFrame:
        # Custom logs loading logic
        pass

# Register custom data source
dm.register_data_source("custom", CustomDataSource())
```

#### Algorithm Composition

```python
from rcabench_platform.v2.algorithms import EnsembleAlgorithm

class MultiAlgorithmEnsemble(EnsembleAlgorithm):
    def __init__(self):
        self.algorithms = [
            ("baro", BaroAlgorithm()),
            ("nsigma", NSigmaAlgorithm()),
            ("custom", CustomAlgorithm())
        ]
    
    def combine_results(self, results: Dict[str, List[AlgorithmResult]]) -> List[AlgorithmResult]:
        # Implement ensemble logic
        pass
```

#### Streaming Data Processing

```python
from rcabench_platform.v2.streaming import StreamProcessor

class RealTimeProcessor(StreamProcessor):
    async def process_stream(self, data_stream):
        async for batch in data_stream:
            # Convert to datapack format
            datapack = self.convert_batch(batch)
            
            # Run algorithm
            results = algorithm(datapack)
            
            # Handle results
            await self.handle_results(results)
```

### Performance Optimization

#### Memory Management

```python
from rcabench_platform.v2.optimization import ChunkedProcessor

# Process large datasets in chunks
processor = ChunkedProcessor(chunk_size=1000)
for chunk in processor.process_dataset("large-dataset"):
    results = algorithm(chunk)
    # Process results incrementally
```

#### Parallel Execution

```python
from rcabench_platform.v2.parallel import ParallelExecutor

# Execute algorithms in parallel
executor = ParallelExecutor(max_workers=4)
results = await executor.run_parallel([
    ("baro", baro_algorithm),
    ("nsigma", nsigma_algorithm),
    ("custom", custom_algorithm)
], datapacks)
```

This API reference provides comprehensive coverage of rcabench-platform's interfaces. For specific usage examples, see the [Examples documentation](./EXAMPLES.md).