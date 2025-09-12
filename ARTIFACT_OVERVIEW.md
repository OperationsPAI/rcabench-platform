# Artifact Overview

This repository contains a comprehensive Root Cause Analysis (RCA) experiment framework designed for research and development of RCA algorithms. The artifact has been prepared for double-blind submission with all personal and organizational information anonymized.

## Anonymization Notice

This artifact has been comprehensively anonymized for double-blind submission:
- All author names and email addresses replaced with generic placeholders
- Organizational references (URLs, IP addresses, server names) replaced with example domains
- Geographic and institutional references anonymized
- Personal comments and mentions removed from code
- Infrastructure configurations use generic hostnames

## Project Structure

```
├── README.md                   # Main project documentation
├── CONTRIBUTING.md            # Development setup and contribution guidelines
├── ARTIFACT_OVERVIEW.md       # This overview document
├── pyproject.toml             # Python project configuration
├── src/                       # Main source code
│   └── rcabench_platform/     # Platform implementation
├── cli/                       # Command-line interface
├── docs/                      # Comprehensive documentation
│   ├── INSTALL.md            # Installation instructions
│   ├── USER_GUIDE.md         # User guide and API reference
│   ├── specifications.md     # Technical specifications
│   ├── workflow-references.md # Workflow documentation
│   └── maintenance.md        # Maintenance guidelines
├── notebooks/                 # Jupyter notebooks for analysis
├── scripts/                   # Utility scripts
├── infra/                     # Infrastructure configuration
└── docker/                    # Docker configurations
```

## Key Features

### 1. Algorithm Development Framework
- Standardized interface for RCA algorithm implementation
- Support for multiple algorithm types and granularities
- Built-in performance metrics and evaluation

### 2. Dataset Management
- Support for multiple RCA datasets
- Standardized data formats (Parquet)
- Automated data loading and preprocessing
- Dataset analysis tools

### 3. Experiment Framework
- Batch experiment execution
- Parallel processing capabilities
- Comprehensive result tracking
- Performance benchmarking

### 4. Visualization and Analysis
- Interactive data visualization
- Algorithm performance comparison
- Dataset statistics and insights
- Export capabilities for results

## Quick Start

1. **Installation**:
   ```bash
   # Clone repository
   git clone https://github.com/anonymous-org/rcabench-platform.git
   cd rcabench-platform
   
   # Install dependencies
   uv sync --all-extras
   ```

2. **Basic Usage**:
   ```bash
   # List available datasets
   python -m rcabench_platform.v2.cli.main dataset list
   
   # List available algorithms
   python -m rcabench_platform.v2.cli.main algorithm list
   
   # Run evaluation
   python -m rcabench_platform.v2.cli.main eval single --algorithm example --dataset sample
   ```

3. **Development Setup**:
   ```bash
   # Run development checks
   just dev
   
   # Start local services
   docker compose up -d
   ```

## Documentation

The artifact includes comprehensive documentation:

- **[Installation Guide](docs/INSTALL.md)**: Step-by-step installation for different environments
- **[User Guide](docs/USER_GUIDE.md)**: Complete API reference and usage examples
- **[Technical Specifications](docs/specifications.md)**: Detailed technical design
- **[Workflow References](docs/workflow-references.md)**: Common workflows and use cases
- **[Development Guide](CONTRIBUTING.md)**: Setup for contributors and developers

## Technical Highlights

### Modular Architecture
- Clean separation between data processing, algorithms, and evaluation
- Plugin-based algorithm registration
- Extensible dataset loaders

### Performance Optimizations
- Parallel execution with configurable worker pools
- Efficient data processing with Polars
- Streaming data interfaces for large datasets

### Research Features
- Comprehensive metrics collection
- Statistical analysis tools
- Reproducible experiment configurations
- Result export in multiple formats

## Dependencies

The project uses modern Python tooling:
- **Python 3.10+**: Core runtime
- **uv**: Dependency management and virtual environments
- **Polars**: High-performance data processing
- **Typer**: Command-line interface
- **Docker**: Containerization and services

## Infrastructure

The platform can be deployed in multiple configurations:
- **Local Development**: Single machine with Docker services
- **Distributed**: Multi-node setup with shared storage
- **Cloud**: Kubernetes-based deployment

All infrastructure references have been anonymized with example domains and generic configurations.

## License and Usage

This artifact is provided for research purposes. All organizational and personal information has been removed to comply with double-blind submission requirements.