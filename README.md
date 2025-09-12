# rcabench-platform

An experiment framework for Root Cause Analysis (RCA), supporting fast development of RCA algorithms and their evaluation on various datasets.

## Overview

This platform provides a comprehensive framework for:

- **Algorithm Development**: Fast prototyping and development of RCA algorithms
- **Dataset Management**: Support for multiple RCA datasets with standardized formats
- **Evaluation Framework**: Automated evaluation and comparison of algorithm performance
- **Batch Processing**: Large-scale experiments with parallel execution
- **Visualization**: Interactive analysis and visualization of results

## Installation

To add this package to another uv-managed project:

```bash
# Install basic package
uv add rcabench-platform

# Install with dataset analysis functionality
uv add "rcabench-platform[analysis]"
```

The `analysis` extra includes additional dependencies like `graphviz` and `matplotlib` needed for the dataset analysis features.

## Quick Start

See the [User Guide](./docs/USER_GUIDE.md) for detailed usage instructions.

## Documentation

+ [Installation Guide](./docs/INSTALL.md): Installation instructions for different environments
+ [User Guide](./docs/USER_GUIDE.md): Complete guide for using rcabench-platform as both a console command and SDK
+ [Development Guide](./CONTRIBUTING.md): How to set up the development environment and contribute to this project
+ [Specifications](./docs/specifications.md): Design details about RCA algorithms and data formats
+ [Workflow References](./docs/workflow-references.md): How to use the functionalities of this project
+ [Maintenance](./docs/maintenance.md): Guidelines for maintaining the project and release procedures

## Related Projects

+ [rcabench](https://github.com/anonymous-org/rcabench)
+ [rca-algo-contrib](https://github.com/anonymous-org/rca-algo-contrib)
+ [rca-algo-random](https://github.com/anonymous-org/rca-algo-random)
