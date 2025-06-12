# rcabench-platform

An experiment framework for Root Cause Analysis (RCA), supporting fast development of RCA algorithms and their evaluation on various datasets.

## Development Guide

### Requirements

#### Operating System

This project is primarily developed and tested on Ubuntu 24.04 LTS or later versions. Other Linux distributions and macOS environments should be compatible with minimal configuration adjustments.

Windows is not officially supported. While some functionality may work in Windows environments (especially through WSL), we cannot guarantee full compatibility or provide dedicated support.

#### Toolchain

|                     Toolchain                      | Version |
| :------------------------------------------------: | :-----: |
|          [uv](https://docs.astral.sh/uv)           | ^0.7.5  |
|       [just](https://github.com/casey/just)        | ^1.21.0 |
|  [Docker Engine](https://docs.docker.com/engine/)  |    *    |
| [Docker Compose](https://docs.docker.com/compose/) |    *    |

#### IDE

Recommended setup

+ [Visual Studio Code](https://code.visualstudio.com/) with the following extensions:
    + [Python Extension Pack](https://marketplace.visualstudio.com/items?itemName=donjayamanne.python-extension-pack)
    + [Pylance](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)
    + [Ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
    + [Git Graph](https://marketplace.visualstudio.com/items?itemName=mhutchie.git-graph)
    + [Data Wrangler](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.datawrangler)

### Git

#### Commit Message

We follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification.

#### Branching Strategy

When you are developing a new feature, create a new branch from `main` and name it according to the following convention:

```
<your github id>/feat/<feature-name>
```

This branch prefixed with your github id, is your own working branch. You can force-push to it freely. Do anything you want in this branch. 

When you are done with the feature, create a pull request to `main` and invite other developers to review your code. If the code is approved, it will be merged into `main`. Then you can start a new branch from `main` and continue your work.

The `main` branch is the default branch for this repository. `main` is protected and should not be used for development. Before merging any changes into `main`, ensure that the following conditions are met:
+ The branch is up to date with `main`.
+ The branch is free of merge conflicts.
+ The basic checks passed successfully.
+ **The changes will not break other developers' workflow.**

We requires a linear commit history. Please use `git rebase` to keep your branch up to date with `main`. When merging your branch into `main`, use `git merge --ff-only` to ensure a fast-forward merge. This will keep the commit history clean and linear.

### Workflow

#### Download source code

```bash
git clone git@github.com:LGU-SE-Internal/rcabench-platform.git
cd rcabench-platform
```

#### Run basic checks

```bash
just dev
```

If the basic checks pass, then your python environment is ready for development.

#### Local development services

```bash
docker compose up -d
```

```bash
docker compose down
```

We have the following localhost services running in the background:
+ [neo4j](https://neo4j.com/): for graph visualization

#### Link datasets

Mount JuiceFS to your machine:

```bash
sudo juicefs mount redis://10.10.10.38:6379/1 /mnt/jfs -d --cache-size=1024
```

See [infra/README.md](infra/README.md) for more details.

Link the datasets to the project directory:

```bash
mkdir -p data
cd data
ln -s /mnt/jfs/rcabench-platform-v2 ./
```

#### Docker image

`rcabench-platform`:

```bash
./scripts/docker.sh build
./scripts/docker.sh run
./scripts/docker.sh push
```

`clickhouse_dataset`:

```bash
cd docker/clickhouse_dataset
./cli.sh build
./cli.sh push
```

### Commands

#### Self test

Test if the environment is set up correctly:

```bash
./main.py self test
```

#### Prepare inputs

```bash
./cli/prepare_inputs.py --help
```

#### Make rcaeval

```bash
./cli/make_rcaeval.py --help
```

#### Make rcabench

```bash
./cli/make_rcabench.py --help
```

Example calls:

```bash
mkdir -p /dev/shm/make
TMP=/dev/shm/make LOGURU_COLORIZE=0 POLARS_MAX_THREADS=16 ./cli/make_rcabench.py run --parallel=8 >temp/a.log 2>&1
```

The example call runs 8 parallel processes with 16 polars threads each, using memory storage as the temporary directory. It is tested on a VM with 128 cores and 192 GiB of RAM.

```bash
./cli/make_rcabench.py make-filtered
./cli/make_rcabench.py make-with-issues
```

```bash
./cli/make_rcabench.py merge-conclusion
```

```bash
./cli/make_rcabench.py query-fault-types rcabench
./cli/make_rcabench.py query-fault-types rcabench_filtered
./cli/make_rcabench.py query-fault-types rcabench_with_issues
```

#### Evaluation

```bash
./main.py eval --help
```

```bash
./main.py eval show-algorithms
./main.py eval show-datasets
```

Example calls:

```bash
./main.py eval single traceback-A7 rcabench_filtered ts3-ts-route-plan-service-request-delay-59s2q4
```

```bash
LOGURU_LEVEL=INFO ./main.py eval batch -d rcaeval_re2_tt -a random -a baro -a nsigma -a traceback-A7 --use-cpus=112 --clear >temp/a.log 2>&1
```

```bash
LOGURU_LEVEL=INFO ./main.py eval batch -d rcabench_filtered -a random -a baro -a nsigma -a traceback-A7 --use-cpus=112 --clear >temp/a.log 2>&1
```

```bash
./main.py eval perf-report rcaeval_re2_tt
./main.py eval perf-report rcabench_filtered
```

### Notebooks

Edit the *SDG Visualization* notebook:

```bash
./notebooks/sdg.py
```
