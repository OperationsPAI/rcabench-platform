# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive documentation for the repository
- Enhanced user guides and API references

## [0.3.33] - Current Release

### Added
- Complete RCA algorithm framework
- Support for multiple dataset formats
- Service Dependency Graph (SDG) functionality
- Docker containerization support
- Kubernetes integration
- Harbor registry support for algorithms
- Analysis and visualization tools
- CLI tools for dataset management and algorithm execution

### Features
- Root Cause Analysis algorithm development framework
- Multiple algorithm evaluation and comparison
- Dataset conversion from various sources (rcabench, RCAEval)
- Real-time monitoring and analysis capabilities
- Interactive tools for algorithm development
- Comprehensive workflow management
- Docker-based algorithm execution
- Online and offline execution modes

### Supported Algorithms
- Random baseline algorithm
- BARO (Bayesian Root Cause Analysis)
- N-Sigma anomaly detection
- Custom algorithm development support

### Supported Data Sources
- rcabench datasets
- RCAEval datasets (RE2-TT, RE2-OB)
- Custom dataset formats
- Telemetry data (traces, metrics, logs)

### Infrastructure
- Kubernetes cluster support
- Docker containerization
- MinIO object storage integration
- ClickHouse database support
- Neo4j graph database integration

## Previous Versions

For versions prior to 0.3.33, please refer to the git commit history.

## Migration Notes

### From 0.2.x to 0.3.x
- Updated CLI interface with new `rca` command
- Enhanced dataset management capabilities
- Improved algorithm registry system

### Breaking Changes
- Some CLI command signatures have changed
- Configuration file format updated
- Algorithm interface modifications

## Security Updates

Security fixes and updates are documented in our security advisories. Please check the repository's security tab for the latest information.