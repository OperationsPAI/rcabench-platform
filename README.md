# rcabench-platform

An experiment framework for Root Cause Analysis (RCA), supporting fast development of RCA algorithms and their evaluation on various datasets.

## Installation

To add this package to another uv-managed project:

```bash
# Install basic package
uv add rcabench-platform

# Install with dataset analysis functionality
uv add "rcabench-platform[analysis]"
```

The `analysis` extra includes additional dependencies like `graphviz` and `matplotlib` needed for the dataset analysis features.

## Documentation

+ [Development Guide](./CONTRIBUTING.md): How to set up the development environment and contribute to this project.
+ [Specifications](./docs/specifications.md): Our design details about RCA algorithms and data formats.
+ [Workflow References](./docs/workflow-references.md): How to use the functionalities of this project.
+ [Maintenance](./docs/maintenance.md): Guidelines for maintaining the project and release procedures.

## Related Projects

+ [rcabench](https://github.com/LGU-SE-Internal/rcabench)
+ [rca-algo-contrib](https://github.com/LGU-SE-Internal/rca-algo-contrib)
+ [rca-algo-random](https://github.com/LGU-SE-Internal/rca-algo-random)
