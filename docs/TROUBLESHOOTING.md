# Troubleshooting Guide

## Installation Issues

### Command Not Found After Installation

**Problem**: `rca: command not found` after installation

**Solutions**:
1. **Check PATH environment variable**:
   ```bash
   echo $PATH
   python -m site --user-base
   export PATH="$PATH:$(python -m site --user-base)/bin"
   ```

2. **Use full path temporarily**:
   ```bash
   $(python -m site --user-base)/bin/rca --help
   ```

3. **Reinstall with user flag**:
   ```bash
   pip install --user rcabench-platform
   ```

### Permission Errors

**Problem**: Permission denied when accessing files or directories

**Solutions**:
1. **Check file permissions**:
   ```bash
   ls -la /path/to/data
   chmod 755 /path/to/data
   ```

2. **Use user installation**:
   ```bash
   pip install --user rcabench-platform
   ```

3. **Create proper directory structure**:
   ```bash
   mkdir -p ~/rcabench-platform/{data,output,temp}
   chmod 755 ~/rcabench-platform
   ```

### Dependency Conflicts

**Problem**: Package dependency conflicts during installation

**Solutions**:
1. **Use virtual environment**:
   ```bash
   python -m venv rcabench-env
   source rcabench-env/bin/activate
   pip install rcabench-platform
   ```

2. **Use uv for better dependency resolution**:
   ```bash
   pip install uv
   uv add rcabench-platform
   ```

3. **Check for conflicting packages**:
   ```bash
   pip check
   pip list --outdated
   ```

## Dataset Issues

### Dataset Not Found

**Problem**: `Dataset 'my-dataset' not found`

**Solutions**:
1. **Check dataset location**:
   ```bash
   ls -la $DATA_ROOT/meta/
   ls -la $DATA_ROOT/data/
   ```

2. **Verify dataset structure**:
   ```bash
   # Should contain index.parquet and labels.parquet
   ls -la $DATA_ROOT/meta/my-dataset/
   ```

3. **Check environment variables**:
   ```bash
   echo $DATA_ROOT
   echo $ENV_MODE
   ```

4. **List available datasets**:
   ```bash
   rca list-datasets
   ```

### Dataset Conversion Failures

**Problem**: Dataset conversion scripts fail or produce incomplete results

**Solutions**:
1. **Check source data format**:
   ```bash
   file /path/to/source/data/*
   head -n 5 /path/to/source/data/file.csv
   ```

2. **Verify disk space**:
   ```bash
   df -h /path/to/output
   ```

3. **Run with increased verbosity**:
   ```bash
   ./cli/make_rcabench.py run --verbose
   ```

4. **Use incremental mode for recovery**:
   ```bash
   # Scripts support recovery from failures
   ./cli/make_rcabench.py run  # Will skip completed datapacks
   ```

### Large Dataset Performance

**Problem**: Slow performance with large datasets

**Solutions**:
1. **Use symlink datasets for subsets**:
   ```bash
   ./cli/make_rcabench_filtered.py run
   ```

2. **Optimize storage backend**:
   ```bash
   # Use faster storage
   export DATA_ROOT=/mnt/ssd/rcabench-data
   ```

3. **Parallel processing**:
   ```bash
   # Use multiple workers
   ./cli/make_rcabench.py run --workers 4
   ```

## Algorithm Execution Issues

### Algorithm Not Found

**Problem**: `Algorithm 'my-algo' not found in registry`

**Solutions**:
1. **List available algorithms**:
   ```bash
   rca list-algorithms
   ```

2. **Check algorithm registration**:
   ```python
   from rcabench_platform.v2.algorithms.spec import global_algorithm_registry
   registry = global_algorithm_registry()
   print(list(registry.keys()))
   ```

3. **Register custom algorithm**:
   ```python
   registry["my-algo"] = MyAlgorithmClass
   ```

### Docker Execution Failures

**Problem**: Algorithm execution fails with Docker errors

**Solutions**:
1. **Check Docker daemon**:
   ```bash
   docker info
   systemctl status docker
   ```

2. **Verify image accessibility**:
   ```bash
   docker pull my-algorithm:latest
   docker images
   ```

3. **Check container logs**:
   ```bash
   docker logs container_name
   ```

4. **Test with simple image**:
   ```bash
   docker run hello-world
   ```

### Memory/Resource Issues

**Problem**: Out of memory or resource limit errors

**Solutions**:
1. **Check system resources**:
   ```bash
   free -h
   df -h
   top
   ```

2. **Increase Docker memory limits**:
   ```bash
   docker run --memory 4g my-algorithm
   ```

3. **Use streaming processing**:
   ```python
   # Process data in chunks
   for chunk in pd.read_csv('large_file.csv', chunksize=1000):
       process_chunk(chunk)
   ```

4. **Monitor resource usage**:
   ```bash
   docker stats
   htop
   ```

## Network and Connectivity Issues

### Connection Timeouts

**Problem**: Timeouts when connecting to remote services

**Solutions**:
1. **Check network connectivity**:
   ```bash
   ping remote-server.com
   curl -I https://remote-server.com
   ```

2. **Configure proxy settings**:
   ```bash
   export HTTP_PROXY=http://proxy:8080
   export HTTPS_PROXY=https://proxy:8080
   ```

3. **Increase timeout values**:
   ```python
   import requests
   response = requests.get(url, timeout=30)
   ```

### SSL/TLS Certificate Issues

**Problem**: SSL certificate verification failures

**Solutions**:
1. **Update certificates**:
   ```bash
   sudo apt update && sudo apt install ca-certificates
   ```

2. **Check certificate validity**:
   ```bash
   openssl s_client -connect remote-server.com:443
   ```

3. **Temporary bypass (development only)**:
   ```python
   import requests
   requests.packages.urllib3.disable_warnings()
   response = requests.get(url, verify=False)  # Only for development!
   ```

## Configuration Issues

### Environment Configuration

**Problem**: Wrong environment mode or configuration

**Solutions**:
1. **Check current configuration**:
   ```python
   from rcabench_platform.v2.config import get_config
   config = get_config()
   print(f"Mode: {config.env_mode}")
   print(f"Data: {config.data}")
   print(f"Output: {config.output}")
   ```

2. **Override environment mode**:
   ```bash
   export ENV_MODE=debug
   ```

3. **Set custom paths**:
   ```bash
   export DATA_ROOT=/custom/data/path
   export OUTPUT_ROOT=/custom/output/path
   ```

### Service Configuration

**Problem**: Cannot connect to required services (ClickHouse, Neo4j, etc.)

**Solutions**:
1. **Check service status**:
   ```bash
   docker-compose ps
   kubectl get pods
   ```

2. **Verify service URLs**:
   ```bash
   curl http://clickhouse:8123/ping
   ```

3. **Check service logs**:
   ```bash
   docker-compose logs clickhouse
   kubectl logs -f deployment/neo4j
   ```

## Development Issues

### Import Errors

**Problem**: Module import failures during development

**Solutions**:
1. **Install in development mode**:
   ```bash
   pip install -e .
   ```

2. **Check Python path**:
   ```python
   import sys
   print(sys.path)
   ```

3. **Verify package installation**:
   ```bash
   pip show rcabench-platform
   pip list | grep rcabench
   ```

### Code Style Issues

**Problem**: Linting or formatting errors

**Solutions**:
1. **Run code formatters**:
   ```bash
   black src/
   ruff format src/
   ```

2. **Fix linting issues**:
   ```bash
   ruff check src/ --fix
   ```

3. **Check type annotations**:
   ```bash
   pyright src/
   ```

### Test Failures

**Problem**: Unit tests failing

**Solutions**:
1. **Run tests with verbose output**:
   ```bash
   python -m pytest -v
   ```

2. **Run specific test**:
   ```bash
   python -m pytest tests/test_specific.py::test_function -v
   ```

3. **Check test dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```

## Performance Issues

### Slow Algorithm Execution

**Problem**: Algorithms taking too long to execute

**Solutions**:
1. **Profile algorithm performance**:
   ```python
   import cProfile
   cProfile.run('algorithm.run(args)')
   ```

2. **Monitor resource usage**:
   ```bash
   top -p $(pgrep -f algorithm)
   ```

3. **Optimize data loading**:
   ```python
   # Use efficient data formats
   df = pd.read_parquet('data.parquet')  # Instead of CSV
   ```

4. **Use parallel processing**:
   ```python
   from multiprocessing import Pool
   with Pool(4) as p:
       results = p.map(process_datapack, datapacks)
   ```

### Memory Leaks

**Problem**: Increasing memory usage over time

**Solutions**:
1. **Monitor memory usage**:
   ```python
   import psutil
   process = psutil.Process()
   print(f"Memory: {process.memory_info().rss / 1024 / 1024} MB")
   ```

2. **Use memory profiling**:
   ```bash
   pip install memory-profiler
   python -m memory_profiler script.py
   ```

3. **Clear variables explicitly**:
   ```python
   del large_dataframe
   import gc
   gc.collect()
   ```

## Platform-Specific Issues

### Kubernetes Issues

**Problem**: Problems with Kubernetes deployment

**Solutions**:
1. **Check cluster status**:
   ```bash
   kubectl cluster-info
   kubectl get nodes
   ```

2. **Verify pod status**:
   ```bash
   kubectl get pods -A
   kubectl describe pod pod-name
   ```

3. **Check resource quotas**:
   ```bash
   kubectl describe quota
   kubectl top nodes
   kubectl top pods
   ```

### Harbor Registry Issues

**Problem**: Cannot push/pull from Harbor registry

**Solutions**:
1. **Check authentication**:
   ```bash
   docker login harbor.example.com
   ```

2. **Verify image name format**:
   ```bash
   docker tag my-algo harbor.example.com/project/my-algo:v1.0
   ```

3. **Check registry connectivity**:
   ```bash
   curl https://harbor.example.com/api/health
   ```

## Debugging Tips

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or for specific modules
logger = logging.getLogger('rcabench_platform')
logger.setLevel(logging.DEBUG)
```

### Use Interactive Debugging

```python
import pdb
pdb.set_trace()  # Set breakpoint

# Or use IPython
import IPython
IPython.embed()
```

### Check Version Information

```bash
rca --version
python --version
docker --version
kubectl version --client
```

### Generate Diagnostic Information

```bash
# System information
uname -a
lscpu
free -h
df -h

# Python environment
pip list
pip check

# Docker information
docker info
docker images
docker ps -a
```

## Getting Help

If you've tried the solutions above and still have issues:

1. **Check existing documentation**:
   - [User Guide](./USER_GUIDE.md)
   - [FAQ](./FAQ.md)
   - [Architecture](./ARCHITECTURE.md)

2. **Search existing issues**:
   - Check the repository issue tracker
   - Look for similar problems and solutions

3. **Prepare a good bug report**:
   - Clear description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - System information and logs
   - Minimal reproducible example

4. **Include relevant information**:
   - Platform version
   - Operating system
   - Python version
   - Configuration details
   - Error messages and logs

5. **Create a new issue** with all the above information

Remember: Good troubleshooting often involves working systematically through potential causes, from the most common to the most specific.