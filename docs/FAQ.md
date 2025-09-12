# Frequently Asked Questions (FAQ)

## General Questions

### What is rcabench-platform?

rcabench-platform is an experiment framework for Root Cause Analysis (RCA) that supports fast development of RCA algorithms and their evaluation on various datasets. It provides both a command-line interface and an SDK for developing and testing RCA algorithms.

### Who should use this platform?

This platform is designed for:
- Researchers working on Root Cause Analysis algorithms
- DevOps engineers implementing RCA solutions
- Data scientists analyzing system failures
- Software engineers developing monitoring solutions
- Students learning about fault detection and diagnosis

### What makes this platform different from other RCA tools?

- **Multi-algorithm support**: Compare different RCA algorithms on the same datasets
- **Standardized datasets**: Convert and work with data from multiple sources
- **Docker integration**: Containerized algorithm execution
- **SDK and CLI**: Both programmatic and command-line interfaces
- **Comprehensive evaluation**: Built-in metrics and analysis tools

## Installation and Setup

### Q: What are the system requirements?

**A:** 
- **Operating System**: Ubuntu 24.04 LTS or later (primary), macOS compatible, Windows not officially supported
- **Python**: 3.10 or higher
- **Tools**: uv, just, Docker Engine, Docker Compose
- **Memory**: At least 4GB RAM recommended
- **Storage**: Variable depending on datasets (can be very large)

### Q: How do I install rcabench-platform?

**A:** Use one of these methods:
```bash
# Using uv (recommended)
uv add rcabench-platform

# Using pip
pip install rcabench-platform

# With analysis features
uv add "rcabench-platform[analysis]"
```

### Q: What if I get "command not found" after installation?

**A:** Ensure pip's installation directory is in your PATH:
```bash
export PATH="$PATH:$(python -m site --user-base)/bin"
```

## Usage Questions

### Q: How do I list available algorithms?

**A:** Use the CLI command:
```bash
rca list-algorithms
```

### Q: How do I add a new dataset?

**A:** Follow the dataset specification in our [specifications document](./specifications.md) and use the conversion tools in the `cli/` directory.

### Q: Can I use custom Docker images for algorithms?

**A:** Yes! You can specify custom Docker images:
```bash
rca submit-execution \
  --algorithm "myregistry.com/my-custom-algo:v1.0" \
  --dataset "my-dataset"
```

### Q: How do I compare multiple algorithms?

**A:** Submit executions for multiple algorithms and use the comparison tools:
```bash
rca submit-execution \
  --algorithm "random" \
  --algorithm "baro" \
  --dataset "test-dataset"

rca multi-metrics \
  --algorithm "random" \
  --algorithm "baro" \
  --dataset "test-dataset"
```

## Development Questions

### Q: How do I develop a custom RCA algorithm?

**A:** Follow these steps:
1. Implement the algorithm class following our specification
2. Register it in the global algorithm registry
3. Test it using the SDK or CLI
4. Package it as a Docker container if needed

See the [User Guide](./USER_GUIDE.md#sdk-usage) for detailed examples.

### Q: What data formats are supported?

**A:** We support:
- **Traces**: OpenTelemetry format with trace_id, span_id, service information
- **Metrics**: Time series data with metric names, values, and service tags
- **Logs**: Structured logs with timestamps, levels, and service information
- **JSON/Text**: Additional metadata and configuration files

### Q: How do I contribute to the project?

**A:** Please read our [Development Guide](../CONTRIBUTING.md) for detailed contribution guidelines including:
- Setting up the development environment
- Code style and standards
- Testing requirements
- Pull request process

## Technical Questions

### Q: How does the online mode work?

**A:** Online mode connects to remote services for:
- Algorithm execution on Kubernetes clusters
- Access to shared datasets
- Result storage and retrieval
- Monitoring and logging

Configure it using environment variables or configuration files.

### Q: What is Service Dependency Graph (SDG)?

**A:** SDG functionality helps visualize and analyze the relationships between microservices in your system. Access it via:
```bash
python main.py sdg --help
```

### Q: How do I handle large datasets?

**A:** Large datasets are stored on network-based storage like NFS or JuiceFS. The platform uses:
- Lazy loading for efficiency
- Incremental processing
- Parallel execution where possible
- Symlink-based dataset subsets to avoid duplication

### Q: Can I run algorithms in parallel?

**A:** Yes, the platform supports:
- Multiple concurrent algorithm executions
- Batch processing of datasets
- Distributed execution on Kubernetes
- Local parallel processing

## Troubleshooting

### Q: My algorithm execution is stuck or failing

**A:** Check these common issues:
1. **Resource constraints**: Ensure sufficient memory and CPU
2. **Docker issues**: Verify Docker is running and images are accessible
3. **Dataset access**: Confirm dataset paths and permissions
4. **Network connectivity**: Check connections to remote services
5. **Algorithm logs**: Review execution logs for specific errors

### Q: How do I debug algorithm performance?

**A:** Use these tools:
- Built-in profiling and metrics
- Algorithm execution logs
- Resource monitoring
- Step-by-step SDK debugging
- Visualization tools for results

### Q: Dataset conversion is failing

**A:** Common solutions:
1. **Check source data format**: Ensure it matches expected schema
2. **Verify file permissions**: Confirm read/write access
3. **Storage space**: Ensure sufficient disk space
4. **Dependencies**: Install required conversion libraries
5. **Incremental processing**: Use recovery features for large conversions

## Integration Questions

### Q: How do I integrate with existing monitoring systems?

**A:** The platform supports:
- REST API integration
- Kubernetes deployment
- Docker container orchestration
- Custom data ingestion pipelines
- Export capabilities for various formats

### Q: Can I use this with my existing ML/AI pipeline?

**A:** Yes, the platform provides:
- Python SDK for programmatic access
- Standardized data formats
- Export capabilities
- Integration with popular ML libraries
- Custom algorithm development support

## Getting Help

### Q: Where can I get more help?

**A:** Resources available:
- **Documentation**: Check the `docs/` directory
- **Examples**: See practical examples in user guides
- **Issues**: Report problems via the issue tracker
- **Community**: Engage with other users and contributors

### Q: How do I report bugs or request features?

**A:** Please use the repository's issue tracker with:
- Clear description of the problem or request
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- System information and logs
- Minimal reproducible examples

### Q: Is there a roadmap for future features?

**A:** Check the repository issues and milestones for planned features and improvements. Community input and contributions are welcome!