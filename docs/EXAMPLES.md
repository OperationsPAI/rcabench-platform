# Examples and Tutorials

## Quick Start Examples

### 1. Basic Algorithm Comparison

Compare multiple built-in algorithms on a test dataset:

```bash
# Submit executions for multiple algorithms
rca submit-execution \
  --algorithm "random" \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "test-dataset" \
  --project "quick-comparison" \
  --tag "tutorial"

# Wait for completion and get results
rca multi-metrics \
  --algorithm "random" \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "test-dataset" \
  --tag "tutorial"
```

### 2. Dataset Analysis

Explore and analyze datasets:

```bash
# List available datasets
rca list-datasets

# Get dataset information
rca dataset-info "rcabench" --version "v1.0"

# Analyze dataset structure
python -c "
from rcabench_platform.v2.datasets import load_dataset_index
index = load_dataset_index('rcabench')
print(f'Dataset contains {len(index)} datapacks')
print(index.head())
"
```

### 3. Custom Algorithm Development

Develop and test a simple custom algorithm:

```python
# custom_algorithm.py
from pathlib import Path
from typing import List
from rcabench_platform.v2.algorithms.spec import AlgorithmArgs, AlgorithmResult

class SimpleThresholdAlgorithm:
    def __init__(self, threshold: float = 0.95):
        self.threshold = threshold
    
    def __call__(self, args: AlgorithmArgs) -> List[AlgorithmResult]:
        # Load metrics data
        metrics_file = args.input_folder / "metrics.parquet"
        if not metrics_file.exists():
            return []
        
        import pandas as pd
        metrics = pd.read_parquet(metrics_file)
        
        # Simple threshold-based detection
        anomalies = []
        for service in metrics['service_name'].unique():
            service_metrics = metrics[metrics['service_name'] == service]
            cpu_metrics = service_metrics[service_metrics['metric'] == 'cpu_usage']
            
            if not cpu_metrics.empty:
                max_cpu = cpu_metrics['value'].max()
                if max_cpu > self.threshold:
                    anomalies.append(AlgorithmResult(
                        service_name=service,
                        score=max_cpu,
                        reason=f"CPU usage {max_cpu:.2f} exceeds threshold {self.threshold}"
                    ))
        
        return anomalies

# Register and test the algorithm
from rcabench_platform.v2.algorithms.spec import global_algorithm_registry

registry = global_algorithm_registry()
registry["simple-threshold"] = SimpleThresholdAlgorithm

# Test locally
if __name__ == "__main__":
    args = AlgorithmArgs(
        dataset="test-dataset",
        datapack="test-datapack",
        input_folder=Path("/tmp/test-input"),
        output_folder=Path("/tmp/test-output")
    )
    
    algorithm = SimpleThresholdAlgorithm(threshold=0.9)
    results = algorithm(args)
    print(f"Found {len(results)} anomalies")
    for result in results:
        print(f"- {result.service_name}: {result.score:.3f} ({result.reason})")
```

## SDK Usage Examples

### Working with Datasets

```python
from rcabench_platform.v2.datasets import DatasetManager
from rcabench_platform.v2.config import get_config

# Initialize dataset manager
config = get_config()
dm = DatasetManager(config)

# Load dataset information
dataset_info = dm.get_dataset_info("rcabench")
print(f"Dataset: {dataset_info.name}")
print(f"Datapacks: {len(dataset_info.datapacks)}")

# Access specific datapack
datapack = dm.load_datapack("rcabench", "datapack-001")
print(f"Traces: {len(datapack.traces)}")
print(f"Metrics: {len(datapack.metrics)}")
print(f"Logs: {len(datapack.logs)}")

# Iterate through all datapacks
for datapack_name in dataset_info.datapacks[:5]:  # First 5
    datapack = dm.load_datapack("rcabench", datapack_name)
    print(f"Processing {datapack_name}: {len(datapack.traces)} traces")
```

### Algorithm Development with Validation

```python
from rcabench_platform.v2.algorithms.spec import AlgorithmArgs, AlgorithmResult
from rcabench_platform.v2.validation import validate_algorithm_results
import pandas as pd
from pathlib import Path

class GraphBasedRCAAlgorithm:
    def __init__(self, correlation_threshold: float = 0.8):
        self.correlation_threshold = correlation_threshold
    
    def build_service_graph(self, traces_df: pd.DataFrame) -> dict:
        """Build service dependency graph from traces"""
        graph = {}
        
        # Group spans by trace_id
        for trace_id, trace_spans in traces_df.groupby('trace_id'):
            spans = trace_spans.sort_values('start_time')
            
            # Build call relationships
            for i in range(len(spans) - 1):
                source = spans.iloc[i]['service_name']
                target = spans.iloc[i + 1]['service_name']
                
                if source not in graph:
                    graph[source] = set()
                graph[source].add(target)
        
        return graph
    
    def calculate_service_scores(self, metrics_df: pd.DataFrame, graph: dict) -> dict:
        """Calculate anomaly scores based on metrics and graph structure"""
        scores = {}
        
        # Calculate basic anomaly scores
        for service in metrics_df['service_name'].unique():
            service_metrics = metrics_df[metrics_df['service_name'] == service]
            
            # Simple anomaly score based on metric variance
            error_metrics = service_metrics[service_metrics['metric'].str.contains('error|fail', case=False, na=False)]
            if not error_metrics.empty:
                error_rate = error_metrics['value'].mean()
                scores[service] = error_rate
            else:
                scores[service] = 0.0
        
        # Propagate scores through graph
        for service, dependencies in graph.items():
            if service in scores:
                base_score = scores[service]
                # Increase score if dependencies also have high scores
                dep_scores = [scores.get(dep, 0) for dep in dependencies]
                if dep_scores:
                    propagated_score = base_score + (sum(dep_scores) / len(dep_scores)) * 0.3
                    scores[service] = min(propagated_score, 1.0)
        
        return scores
    
    def __call__(self, args: AlgorithmArgs) -> List[AlgorithmResult]:
        # Load data
        traces_file = args.input_folder / "traces.parquet"
        metrics_file = args.input_folder / "metrics.parquet"
        
        if not traces_file.exists() or not metrics_file.exists():
            return []
        
        traces_df = pd.read_parquet(traces_file)
        metrics_df = pd.read_parquet(metrics_file)
        
        # Build service dependency graph
        graph = self.build_service_graph(traces_df)
        
        # Calculate service scores
        scores = self.calculate_service_scores(metrics_df, graph)
        
        # Generate results
        results = []
        for service, score in scores.items():
            if score > self.correlation_threshold:
                results.append(AlgorithmResult(
                    service_name=service,
                    score=score,
                    reason=f"Graph-based analysis: score {score:.3f}, dependencies: {len(graph.get(service, []))}"
                ))
        
        # Sort by score descending
        results.sort(key=lambda x: x.score, reverse=True)
        
        return results

# Example usage with validation
def test_graph_algorithm():
    # Create test algorithm
    algorithm = GraphBasedRCAAlgorithm(correlation_threshold=0.6)
    
    # Create test data
    test_args = AlgorithmArgs(
        dataset="test-dataset",
        datapack="test-datapack",
        input_folder=Path("/tmp/test-input"),
        output_folder=Path("/tmp/test-output")
    )
    
    # Run algorithm
    results = algorithm(test_args)
    
    # Validate results
    validation_results = validate_algorithm_results(results)
    print(f"Validation passed: {validation_results.is_valid}")
    
    if not validation_results.is_valid:
        for error in validation_results.errors:
            print(f"Validation error: {error}")
    
    return results

# Register algorithm
from rcabench_platform.v2.algorithms.spec import global_algorithm_registry
registry = global_algorithm_registry()
registry["graph-based-rca"] = GraphBasedRCAAlgorithm
```

### Batch Processing Example

```python
import asyncio
from rcabench_platform.v2.execution import ExecutionEngine
from rcabench_platform.v2.config import get_config

async def batch_algorithm_evaluation():
    """Run multiple algorithms on multiple datasets"""
    
    config = get_config()
    engine = ExecutionEngine(config)
    
    # Define evaluation matrix
    algorithms = ["random", "baro", "graph-based-rca"]
    datasets = ["rcabench_filtered", "rcaeval_re2_tt"]
    
    results = {}
    
    for dataset in datasets:
        for algorithm in algorithms:
            print(f"Running {algorithm} on {dataset}")
            
            execution_id = await engine.submit_execution(
                algorithm=algorithm,
                dataset=dataset,
                project="batch-evaluation",
                tag=f"{algorithm}-{dataset}"
            )
            
            # Wait for completion
            result = await engine.wait_for_completion(execution_id)
            results[f"{algorithm}-{dataset}"] = result
            
            print(f"Completed: {algorithm} on {dataset}, score: {result.score:.3f}")
    
    return results

# Run batch evaluation
if __name__ == "__main__":
    results = asyncio.run(batch_algorithm_evaluation())
    
    # Analyze results
    print("\nBatch Evaluation Results:")
    for key, result in results.items():
        print(f"{key}: {result.score:.3f}")
```

## Docker Integration Examples

### Building Custom Algorithm Container

```dockerfile
# Dockerfile for custom algorithm
FROM python:3.10-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy algorithm code
COPY algorithm/ /app/algorithm/
COPY entrypoint.sh /app/

# Set working directory
WORKDIR /app

# Create non-root user
RUN useradd --create-home --shell /bin/bash algo-user
USER algo-user

# Make entrypoint executable
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
```

```bash
#!/bin/bash
# entrypoint.sh
set -e

# Validate input arguments
if [ $# -ne 4 ]; then
    echo "Usage: $0 <dataset> <datapack> <input_folder> <output_folder>"
    exit 1
fi

DATASET=$1
DATAPACK=$2
INPUT_FOLDER=$3
OUTPUT_FOLDER=$4

# Create output directory
mkdir -p "$OUTPUT_FOLDER"

# Run algorithm
python -m algorithm.main \
    --dataset "$DATASET" \
    --datapack "$DATAPACK" \
    --input "$INPUT_FOLDER" \
    --output "$OUTPUT_FOLDER"

echo "Algorithm completed successfully"
```

```python
# algorithm/main.py
import argparse
import json
from pathlib import Path
from algorithm.core import MyCustomAlgorithm

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--datapack', required=True)
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    
    args = parser.parse_args()
    
    # Run algorithm
    algorithm = MyCustomAlgorithm()
    results = algorithm.run(args.input)
    
    # Save results
    output_file = args.output / "results.json"
    with open(output_file, 'w') as f:
        json.dump([r.to_dict() for r in results], f, indent=2)
    
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
```

### Build and Test

```bash
# Build Docker image
docker build -t my-custom-algorithm:v1.0 .

# Test locally
docker run --rm \
  -v /path/to/test/data:/input \
  -v /path/to/output:/output \
  my-custom-algorithm:v1.0 \
  test-dataset test-datapack /input /output

# Push to registry
docker tag my-custom-algorithm:v1.0 harbor.example.com/algorithms/my-custom-algorithm:v1.0
docker push harbor.example.com/algorithms/my-custom-algorithm:v1.0
```

### Using with rcabench-platform

```bash
# Upload algorithm to Harbor registry
rca upload-algorithm-harbor /path/to/algorithm/folder

# Use the algorithm
rca submit-execution \
  --algorithm "harbor.example.com/algorithms/my-custom-algorithm:v1.0" \
  --dataset "rcabench" \
  --project "custom-algo-test"
```

## Advanced Examples

### Real-time Monitoring Integration

```python
from rcabench_platform.v2.online import OnlineRCAProcessor
from rcabench_platform.v2.streaming import DataStream
import asyncio

class RealTimeRCAMonitor:
    def __init__(self):
        self.processor = OnlineRCAProcessor()
        self.algorithms = ["baro", "nsigma"]
    
    async def process_telemetry_stream(self, stream: DataStream):
        """Process incoming telemetry data in real-time"""
        
        async for batch in stream:
            # Convert streaming data to datapack format
            datapack = self.convert_to_datapack(batch)
            
            # Run multiple algorithms
            results = {}
            for algorithm in self.algorithms:
                result = await self.processor.analyze(algorithm, datapack)
                results[algorithm] = result
            
            # Detect anomalies
            anomalies = self.detect_consensus_anomalies(results)
            
            if anomalies:
                await self.send_alerts(anomalies)
                print(f"Anomalies detected: {len(anomalies)}")
    
    def convert_to_datapack(self, batch):
        """Convert streaming data to datapack format"""
        # Implementation depends on your data format
        pass
    
    def detect_consensus_anomalies(self, results):
        """Detect anomalies based on algorithm consensus"""
        consensus_threshold = 0.6  # 60% of algorithms must agree
        
        service_scores = {}
        for algorithm, result in results.items():
            for service_result in result:
                service = service_result.service_name
                if service not in service_scores:
                    service_scores[service] = []
                service_scores[service].append(service_result.score)
        
        anomalies = []
        for service, scores in service_scores.items():
            # Calculate consensus score
            high_score_count = sum(1 for score in scores if score > 0.7)
            consensus_ratio = high_score_count / len(scores)
            
            if consensus_ratio >= consensus_threshold:
                avg_score = sum(scores) / len(scores)
                anomalies.append({
                    'service': service,
                    'score': avg_score,
                    'consensus': consensus_ratio,
                    'algorithms': len(scores)
                })
        
        return anomalies
    
    async def send_alerts(self, anomalies):
        """Send alerts for detected anomalies"""
        for anomaly in anomalies:
            print(f"ALERT: {anomaly['service']} - Score: {anomaly['score']:.3f}")
            # Implement your alerting mechanism here

# Usage
async def main():
    monitor = RealTimeRCAMonitor()
    
    # Connect to your data stream
    # stream = DataStream.from_kafka("telemetry-topic")
    # stream = DataStream.from_clickhouse("SELECT * FROM telemetry")
    
    # await monitor.process_telemetry_stream(stream)

if __name__ == "__main__":
    asyncio.run(main())
```

### Multi-Environment Deployment Example

```python
from rcabench_platform.v2.config import Config, set_config
from rcabench_platform.v2.deployment import MultiEnvironmentDeployer

class ProductionDeploymentExample:
    def __init__(self):
        self.environments = {
            'dev': Config(
                env_mode='dev',
                data=Path('/mnt/dev-data'),
                output=Path('/mnt/dev-output'),
                temp=Path('/tmp/dev'),
                base_url='http://dev-cluster:8082'
            ),
            'staging': Config(
                env_mode='staging',
                data=Path('/mnt/staging-data'),
                output=Path('/mnt/staging-output'),
                temp=Path('/tmp/staging'),
                base_url='http://staging-cluster:8082'
            ),
            'prod': Config(
                env_mode='prod',
                data=Path('/mnt/prod-data'),
                output=Path('/mnt/prod-output'),
                temp=Path('/tmp/prod'),
                base_url='http://prod-cluster:8082'
            )
        }
    
    async def deploy_algorithm_pipeline(self, algorithm_name: str):
        """Deploy algorithm across multiple environments"""
        
        # Test in dev environment
        set_config(self.environments['dev'])
        dev_results = await self.run_validation_tests(algorithm_name)
        
        if not dev_results.passed:
            print(f"Dev tests failed: {dev_results.errors}")
            return False
        
        # Deploy to staging
        set_config(self.environments['staging'])
        staging_results = await self.run_integration_tests(algorithm_name)
        
        if not staging_results.passed:
            print(f"Staging tests failed: {staging_results.errors}")
            return False
        
        # Deploy to production
        set_config(self.environments['prod'])
        prod_deployment = await self.deploy_to_production(algorithm_name)
        
        return prod_deployment.success
    
    async def run_validation_tests(self, algorithm_name: str):
        """Run validation tests in dev environment"""
        # Implementation for dev testing
        pass
    
    async def run_integration_tests(self, algorithm_name: str):
        """Run integration tests in staging environment"""
        # Implementation for staging testing
        pass
    
    async def deploy_to_production(self, algorithm_name: str):
        """Deploy to production environment"""
        # Implementation for production deployment
        pass
```

These examples demonstrate various use cases and integration patterns for rcabench-platform. Start with the basic examples and gradually work your way up to more complex scenarios based on your specific requirements.