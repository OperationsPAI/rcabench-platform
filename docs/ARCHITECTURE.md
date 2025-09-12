# System Architecture

## Overview

rcabench-platform is designed as a modular, scalable framework for Root Cause Analysis (RCA) algorithm development and evaluation. The architecture supports both local development and distributed execution across multiple environments.

## Core Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    rcabench-platform                        │
├─────────────────────────────────────────────────────────────┤
│  CLI Interface          │        SDK Interface              │
│  ┌─────────────────┐    │    ┌─────────────────────────────┐ │
│  │ rca commands    │    │    │ Python API                   │ │
│  │ - list-*        │    │    │ - Algorithm development     │ │
│  │ - submit-*      │    │    │ - Dataset manipulation     │ │
│  │ - query-*       │    │    │ - Result analysis          │ │
│  └─────────────────┘    │    └─────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Core Components                          │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Algorithm   │  │ Dataset     │  │ Execution Engine    │  │
│  │ Registry    │  │ Management  │  │ - Local execution   │  │
│  │ - Built-in  │  │ - Conversion│  │ - Docker execution  │  │
│  │ - Custom    │  │ - Validation│  │ - K8s execution     │  │
│  │ - Docker    │  │ - Indexing  │  │ - Result collection │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                    Data Layer                               │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Datasets    │  │ Results     │  │ Configuration       │  │
│  │ - rcabench  │  │ - Metrics   │  │ - Environment vars  │  │
│  │ - RCAEval   │  │ - Logs      │  │ - Config files      │  │
│  │ - Custom    │  │ - Artifacts │  │ - Runtime settings  │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                 Infrastructure                              │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Storage     │  │ Compute     │  │ Services            │  │
│  │ - Local FS  │  │ - Local     │  │ - MinIO             │  │
│  │ - NFS       │  │ - Docker    │  │ - ClickHouse        │  │
│  │ - JuiceFS   │  │ - K8s       │  │ - Neo4j             │  │
│  │ - MinIO     │  │ - Harbor    │  │ - Kubernetes        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Interface Layer

#### CLI Interface (`rca` command)
- **Purpose**: Command-line access to all platform features
- **Key Commands**:
  - `list-datasets`, `list-algorithms`: Discovery
  - `submit-execution`: Algorithm execution
  - `query-injection`: Data querying
  - `upload-algorithm-harbor`: Algorithm deployment

#### SDK Interface
- **Purpose**: Programmatic access for development and integration
- **Key Features**:
  - Algorithm development framework
  - Dataset manipulation APIs
  - Result analysis tools
  - Custom workflow creation

### 2. Core Components

#### Algorithm Registry
```python
# Algorithm registration system
from rcabench_platform.v2.algorithms.spec import global_algorithm_registry

registry = global_algorithm_registry()
registry["my-algorithm"] = MyAlgorithmClass
```

**Built-in Algorithms**:
- `random`: Baseline random algorithm
- `baro`: Bayesian Root Cause Analysis
- `nsigma`: N-Sigma anomaly detection

**Custom Algorithm Support**:
- Docker-based algorithms
- Local Python algorithms
- Harbor registry integration

#### Dataset Management
```
Dataset Structure:
{ROOT}/
├── meta/{dataset_name}/
│   ├── index.parquet     # Datapack listings
│   └── labels.parquet    # Ground truth labels
└── data/{dataset_name}/
    └── {datapack_name}/
        ├── traces.parquet
        ├── metrics.parquet
        ├── logs.parquet
        └── *.json
```

**Dataset Types**:
- **Full datasets**: Complete data conversion
- **Symlink datasets**: Efficient subsets using symbolic links
- **Filtered datasets**: Processed and cleaned data

#### Execution Engine

**Local Execution**:
```python
# Direct algorithm execution
algorithm = MyAlgorithm()
results = algorithm(args)
```

**Docker Execution**:
```bash
# Containerized execution
rca submit-execution \
  --algorithm "my-docker-algo:latest" \
  --dataset "my-dataset"
```

**Kubernetes Execution**:
- Distributed processing
- Resource management
- Scaling capabilities
- Fault tolerance

### 3. Data Layer

#### Dataset Specification
```python
@dataclass
class AlgorithmArgs:
    dataset: str
    datapack: str
    input_folder: Path
    output_folder: Path
```

**Data Formats**:
- **Traces**: OpenTelemetry format with service topology
- **Metrics**: Time-series with service labels
- **Logs**: Structured logs with trace correlation
- **Metadata**: JSON configuration and labels

#### Result Management
```
Results Structure:
{OUTPUT}/
├── executions/
│   └── {execution_id}/
│       ├── results.json
│       ├── metrics.json
│       └── logs/
└── analysis/
    ├── comparisons/
    └── visualizations/
```

### 4. Infrastructure

#### Storage Systems
- **Local Filesystem**: Development and small datasets
- **Network Storage (NFS/JuiceFS)**: Large-scale datasets
- **Object Storage (MinIO)**: Algorithm artifacts and results
- **Database Storage**: Metadata and indices

#### Service Dependencies
- **ClickHouse**: Telemetry data storage and querying
- **Neo4j**: Graph-based service dependency modeling
- **Harbor**: Container registry for algorithms
- **Kubernetes**: Container orchestration

## Data Flow

### Algorithm Development Flow
```
1. Developer writes algorithm
2. Register in algorithm registry
3. Test with local data
4. Package as Docker container
5. Upload to Harbor registry
6. Execute via platform
```

### Dataset Processing Flow
```
1. Raw data ingestion
2. Format conversion
3. Validation and indexing
4. Storage in platform format
5. Symlink subset creation
6. Ready for algorithm execution
```

### Execution Flow
```
1. Algorithm selection
2. Dataset preparation
3. Execution environment setup
4. Algorithm execution
5. Result collection
6. Analysis and visualization
```

## Configuration Management

### Environment Modes
- **Debug**: Local development with minimal resources
- **Dev**: Development servers with shared resources
- **Prod**: Production environment with full infrastructure

### Configuration Files
```python
@dataclass
class Config:
    env_mode: str
    data: Path          # Dataset storage location
    output: Path        # Results output location
    temp: Path          # Temporary files location
    base_url: str       # Service endpoint
```

## Security Architecture

### Data Protection
- Dataset access controls
- Execution sandboxing
- Result isolation
- Secure container execution

### Network Security
- Service-to-service authentication
- Encrypted data transmission
- Network isolation
- Access logging

## Scalability Considerations

### Horizontal Scaling
- Multiple execution nodes
- Distributed dataset storage
- Load balancing
- Auto-scaling capabilities

### Vertical Scaling
- Resource-aware scheduling
- Memory optimization
- CPU allocation
- Storage optimization

## Monitoring and Observability

### Execution Monitoring
- Real-time execution status
- Resource utilization tracking
- Error detection and alerting
- Performance metrics

### System Health
- Service availability monitoring
- Data integrity checks
- Storage capacity tracking
- Network connectivity monitoring

## Integration Points

### External Systems
- **Monitoring Systems**: Data ingestion APIs
- **CI/CD Pipelines**: Automated testing and deployment
- **Analytics Platforms**: Result export capabilities
- **Notification Systems**: Alert and status updates

### API Interfaces
- REST APIs for remote access
- GraphQL for complex queries
- WebSocket for real-time updates
- Batch processing APIs

## Deployment Patterns

### Single Node Deployment
```yaml
# docker-compose.yml
services:
  platform:
    image: rcabench-platform
    volumes:
      - ./data:/data
      - ./output:/output
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rcabench-platform
spec:
  replicas: 3
  selector:
    matchLabels:
      app: rcabench-platform
```

### Hybrid Deployment
- Local development environment
- Remote execution infrastructure
- Shared dataset storage
- Centralized result collection

This architecture provides flexibility, scalability, and maintainability while supporting diverse use cases from research and development to production deployment.