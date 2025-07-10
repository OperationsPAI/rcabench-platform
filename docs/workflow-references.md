# Workflow References

This document describes the workflows of this project.

## Datasets

Besides our own data, we also support converting data from other projects.

Currently, we have the following data sources:
+ [rcabench](https://github.com/LGU-SE-Internal/rcabench) (our own)
+ [RCAEval](https://github.com/phamquiluan/RCAEval)

As the [development guide](../CONTRIBUTING.md#link-datasets) describes, the data sources and converted datasets are stored in the shared file system, e.g., `/mnt/jfs`.

| data source        | converted dataset                                | subset mode           | generator                                                               |
| ------------------ | ------------------------------------------------ | --------------------- | ----------------------------------------------------------------------- |
| `rcabench_dataset` | `rcabench-platform-v2/data/rcabench`             | full                  | [cli/make_rcabench.py](../cli/make_rcabench.py)                         |
|                    | `rcabench-platform-v2/data/rcabench_filtered`    | symlink to `rcabench` | [cli/make_rcabench_filtered.py](../cli/make_rcabench_filtered.py)       |
|                    | `rcabench-platform-v2/data/rcabench_with_issues` | symlink to `rcabench` | [cli/make_rcabench_with_issues.py](../cli/make_rcabench_with_issues.py) |
| `RCAEval/RE2-TT`   | `rcabench-platform-v2/data/rcaeval_re2_tt`       | full                  | [cli/make_rcaeval.py](../cli/make_rcaeval.py)                           |
| `RCAEval/RE2-OB`   | `rcabench-platform-v2/data/rcaeval_re2_ob`       | full                  | [cli/make_rcaeval.py](../cli/make_rcaeval.py)                           |

The `subset mode` indicates how the dataset is generated:
+ `full`: the dataset is converted from the original data source.
+ `symlink`: the dataset is a subset of another dataset, and the datapacks in it are symlinked to the original dataset.

### Run existing generators

All of the generators support incremental updates and recovery from failures. You can kill the processes at any time, and they can skip the finished datapacks when you run them again.

#### rcabench

Usages:

```bash
./cli/make_rcabench.py --help
```

Run the generator to convert the `rcabench` dataset (slowly):

```bash
./cli/make_rcabench.py run --help
./cli/make_rcabench.py run
```

To accelerate the conversion, here is an example:

```bash
mkdir -p /dev/shm/make
TMP=/dev/shm/make LOGURU_COLORIZE=0 POLARS_MAX_THREADS=16 ./cli/make_rcabench.py run --parallel=8 >temp/a.log 2>&1
```

The example call runs 8 parallel processes with 16 polars threads each, using memory storage as the temporary directory. It is tested on a VM with 128 cores and 192 GiB of RAM.

#### rcabench_filtered

Usages:

```bash
./cli/make_rcabench_filtered.py --help
```

Run the generator:

```bash
./cli/make_rcabench_filtered.py run --help
./cli/make_rcabench_filtered.py run
```

#### rcabench_with_issues

Usages:

```bash
./cli/make_rcabench_with_issues.py --help
```

Run the generator:

```bash
./cli/make_rcabench_with_issues.py run --help
./cli/make_rcabench_with_issues.py run
```

#### rcaeval

Usages:

```bash
./cli/make_rcaeval.py --help
```

Run the generator:

```bash
./cli/make_rcaeval.py run --help
./cli/make_rcaeval.py run
```

Run local tests:

```bash
./cli/make_rcaeval.py local-test-1
./cli/make_rcaeval.py local-test-2
```

### How to add a new dataset

① Download the original data source and put it in the shared file system, e.g., `/mnt/jfs/RCAEval`.

② Add a new python file in the [sources](../src/rcabench_platform/v2/sources/) module. Write your dataset loader in it, following the existing dataset loaders as examples.

③ Add a new script file in the [cli](../cli/) folder to convert the dataset. Make sure that the generator script has a local test function and it runs successfully.

④ Submit a pull request.

Note that:
+ A single datapack is a single fault case.
+ A single datapack has one label typically, which is the root cause of the fault case. But it can also have multiple labels when the fault case has multiple root causes.
+ The generator script is named `cli/make_{dataset}.py`, where `{dataset}` is the name of the dataset or the common prefix of multiple datasets.
+ The dataset name can only contains letters, digits, dashes, and underscores.

## Algorithms

Currently, we have the following algorithms:
+ [random](../src/rcabench_platform/v2/algorithms/random_.py)
+ [traceback-A7](../src/rcabench_platform/v2/algorithms/traceback/A7.py) (our own)
+ [traceback-A8](../src/rcabench_platform/v2/algorithms/traceback/A8.py) (our own)
+ [baro](../src/rcabench_platform/v2/algorithms/rcaeval/baro.py)
+ [nsigma](../src/rcabench_platform/v2/algorithms/rcaeval/nsigma.py)

Algorithms can also be implemented in standalone repositories. Here is an example:
+ <https://github.com/LGU-SE-Internal/rca-algo-random>

### How to add a new builtin algorithm

① Add a new python module in the [algorithms](../src/rcabench_platform/v2/algorithms/) module. Write your algorithm in it, following the existing algorithms as examples.

② Add a new entry in the `register_builtin_algorithms` function in [cli/main.py](../src/rcabench_platform/v2/cli/main.py)

## Evaluation

Usages:

```bash
./main.py eval --help
```

Show available algorithms and datasets:

```bash
./main.py eval show-algorithms
./main.py eval show-datasets
```

Run evaluation on a specific dataset with a specific algorithm:

```bash
./main.py eval single --help
# example
./main.py eval single traceback-A7 rcabench_filtered ts3-ts-route-plan-service-request-delay-59s2q4 --clear
```

Run evaluation on multiple datasets with multiple algorithms:

```bash
./main.py eval batch --help
# example
LOGURU_LEVEL=INFO ./main.py eval batch -d rcaeval_re2_tt    -a random -a baro -a nsigma -a traceback-A7 --use-cpus=112 --clear >temp/a.log 2>&1
LOGURU_LEVEL=INFO ./main.py eval batch -d rcabench_filtered -a random -a baro -a nsigma -a traceback-A7 --use-cpus=112 --clear >temp/a.log 2>&1
```

Generate the performance report:

```bash
./main.py eval perf-report --help
# example
./main.py eval perf-report rcaeval_re2_tt
./main.py eval perf-report rcabench_filtered
```

### Debugging Algorithms

Re-run single evaluation in debug mode:

```bash
# example
DEBUG=1 ./main.py eval single traceback-A7 rcabench_filtered ts3-ts-route-plan-service-request-delay-59s2q4 --no-skip-finished
```

This call will run the evaluation in debug mode without clearing the existing output directory.

The algorithms can be accelerated by caching intermediate calculations in the output directory. However, you should enable caching only when debugging the algorithm, as it may cause issues when the algorithm implementation changes.

## Analysis

### SDG Visualization

Edit the SDG notebook to visualize datapacks:

```bash
./notebooks/sdg.py
```
