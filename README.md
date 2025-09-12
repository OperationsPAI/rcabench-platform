# rcabench-platform

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Version](https://img.shields.io/badge/version-0.3.33-green.svg)

An advanced experiment framework for Root Cause Analysis (RCA) that supports rapid development, evaluation, and deployment of RCA algorithms across diverse datasets and environments.

## 🚀 Quick Start

### Installation

```bash
# Install basic package
pip install rcabench-platform

# Install with analysis features (includes graphviz, matplotlib)
pip install "rcabench-platform[analysis]"

# Install with uv (recommended for development)
uv add rcabench-platform
```

### Basic Usage

```bash
# List available algorithms and datasets
rca list-algorithms
rca list-datasets

# Run a quick algorithm comparison
rca submit-execution \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "test-dataset" \
  --project "quick-test"

# Check results
rca multi-metrics \
  --algorithm "baro" \
  --algorithm "nsigma" \
  --dataset "test-dataset"
```

## ✨ Key Features

### 🔬 **Multi-Algorithm Support**
- **Built-in algorithms**: Random baseline, BARO, N-Sigma
- **Custom algorithms**: Easy Python SDK for development
- **Docker integration**: Containerized algorithm execution
- **Algorithm comparison**: Side-by-side evaluation and metrics

### 📊 **Comprehensive Dataset Support**
- **Multiple sources**: rcabench, RCAEval, custom formats
- **Standardized format**: Traces, metrics, logs in Parquet format
- **Large-scale processing**: Network storage support (NFS, JuiceFS)
- **Efficient subsets**: Symlink-based dataset filtering

### 🛠 **Flexible Execution**
- **Local development**: Direct Python execution
- **Containerized**: Docker-based algorithm isolation
- **Distributed**: Kubernetes cluster execution
- **Online/Offline modes**: Real-time and batch processing

### 📈 **Advanced Analysis**
- **Service Dependency Graphs**: Visualize system topology
- **Multi-metric evaluation**: Precision, recall, F1-score
- **Comparative analysis**: Algorithm performance comparison
- **Export capabilities**: Results in multiple formats

## 🏗 **Architecture**

rcabench-platform follows a modular architecture designed for scalability and extensibility:

```
┌─────────────────────────────────────────────────────────────┐
│                    CLI & SDK Interfaces                     │
├─────────────────────────────────────────────────────────────┤
│  Algorithm Registry  │  Dataset Management  │  Execution    │
├─────────────────────────────────────────────────────────────┤
│         Data Layer (Parquet, JSON, Metadata)               │
├─────────────────────────────────────────────────────────────┤
│    Infrastructure (Docker, K8s, Storage, Services)         │
└─────────────────────────────────────────────────────────────┘
```

## 📚 **Comprehensive Documentation**

### 📖 **User Documentation**
- **[User Guide](./docs/USER_GUIDE.md)**: Complete guide for using rcabench-platform
- **[Installation Guide](./docs/INSTALL.md)**: Detailed installation instructions
- **[Examples & Tutorials](./docs/EXAMPLES.md)**: Practical examples and use cases
- **[FAQ](./docs/FAQ.md)**: Frequently asked questions and solutions
- **[Troubleshooting](./docs/TROUBLESHOOTING.md)**: Common issues and fixes

### 🔧 **Developer Documentation**
- **[API Reference](./docs/API_REFERENCE.md)**: Complete API documentation
- **[Development Guide](./CONTRIBUTING.md)**: Setup and contribution guidelines
- **[Architecture](./docs/ARCHITECTURE.md)**: System design and components
- **[Specifications](./docs/specifications.md)**: Data formats and algorithm specs

### 🔒 **Operations Documentation**
- **[Security Guidelines](./docs/SECURITY.md)**: Security best practices
- **[Workflow References](./docs/workflow-references.md)**: Operational workflows
- **[Maintenance Guide](./docs/maintenance.md)**: Project maintenance procedures
- **[Changelog](./CHANGELOG.md)**: Version history and changes

## 🛡 **Security & Quality**

- **Secure by design**: Container isolation, access controls
- **Code quality**: Comprehensive linting, type checking
- **Testing**: Extensive test suite and validation
- **Documentation**: Complete API and user documentation

## 🤝 **Getting Help & Contributing**

### 📞 **Support**
- Check our [FAQ](./docs/FAQ.md) for common questions
- Review [Troubleshooting Guide](./docs/TROUBLESHOOTING.md) for issues
- Browse existing issues in the repository
- See [User Guide](./docs/USER_GUIDE.md) for detailed usage

### 🎯 **Contributing**
We welcome contributions! Please see our [Development Guide](./CONTRIBUTING.md) for:
- Setting up development environment
- Code style and standards
- Testing requirements
- Pull request process

## 🌟 **Use Cases**

### 🔍 **Research & Development**
- Algorithm prototyping and testing
- Performance comparison studies
- Dataset analysis and validation
- Academic research support

### 🏢 **Enterprise Operations**
- Production RCA deployment
- Real-time anomaly detection
- System monitoring integration
- DevOps workflow automation

### 📚 **Education & Learning**
- RCA algorithm education
- Hands-on learning platform
- Research project foundation
- Algorithm benchmarking

## 🔗 **Related Projects**

Explore our ecosystem of RCA tools and algorithms:

- **[rcabench](https://github.com/anonymous-org/rcabench)**: Core benchmarking platform
- **[rca-algo-contrib](https://github.com/anonymous-org/rca-algo-contrib)**: Community algorithms
- **[rca-algo-random](https://github.com/anonymous-org/rca-algo-random)**: Baseline implementations

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## 🏷 **Version Information**

- **Current Version**: 0.3.33
- **Python Support**: 3.10+
- **Operating Systems**: Ubuntu 24.04+ (primary), macOS (compatible)
- **Last Updated**: 2024

---

**Ready to get started?** Check out our [Quick Start Guide](./docs/USER_GUIDE.md#installation) or explore the [Examples](./docs/EXAMPLES.md) to see rcabench-platform in action!
