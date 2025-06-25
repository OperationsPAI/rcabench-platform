# Docker Images

This project manages several Docker images to support its functionality.

### rcabench-platform

The **fundamental** image containing the project code and dependencies.

Build and push:

```bash
./scripts/docker.sh build
./scripts/docker.sh push
```

This image serves as the base for other images and **should be built first**.

### clickhouse_dataset

The image for collecting telemetry data from ClickHouse.

It is used by [rcabench](https://github.com/LGU-SE-Internal/rcabench) services.

Build and push:

```bash
cd docker/clickhouse_dataset
./cli.sh build
./cli.sh push
```

### detector

The image for detecting SLI anomalies in rcabench data.

It is used by [rcabench](https://github.com/LGU-SE-Internal/rcabench) services.

Build and push:

```bash
cd docker/detector
./cli.sh build
./cli.sh push
```
