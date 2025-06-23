# Specifications

## RCA Algorithm Specification

An instance of an RCA algorithm class is initialized with its configuration (hyperparameters).

The instance takes arguments and returns a list of answers.

The algorithm arguments contain
+ dataset name
+ datapack name
+ input directory containing the data files of the datapack
+ output directory for storing intermediate results

The algorithm answer is a predicted root cause with level, name and rank.

## Data Specification

### Dataset and Datapack

A **dataset** is a collection of datapacks.

A **datapack** is a collection of data files (traces, metrics, logs, json, txt, ...).

A datapack represents the telemetry data of a single fault case in a microservice system.

The datapacks in the same dataset must have compatible file structures.

A dataset must have at least two metadata files:

+ `index.parquet` - list of datapacks in the dataset
+ `labels.parquet` - list of ground truth labels for the datapacks

### File System Structure

The metadata files of a dataset is stored in `{ROOT}/meta/{dataset_name}/`.

The datapacks of a dataset are stored in `{ROOT}/data/{dataset_name}/`.

A datapack is stored in `{ROOT}/data/{dataset_name}/{datapack_name}/`.

We access the files in a datapack through POSIX filesystem API.

The size of a dataset may be very large, so we have to use network-based storages like NFS or JuiceFS to store the data files.

### File Contents

#### Traces

Traces file contains a time series of spans.

|     Column     |   Type   | Description                                              |
| :------------: | :------: | :------------------------------------------------------- |
|      time      | datetime | start time of a span in UTC                              |
|    trace_id    |  string  | unique identifier of a trace (a trace groups many spans) |
|    span_id     |  string  | unique identifier of a span                              |
| parent_span_id |  string  | identifier of the parent span (for trace hierarchy)      |
|  service_name  |  string  | name of the service that generated the span              |
|   span_name    |  string  | name of the operation represented by the span            |
|    duration    |  uint64  | duration of a span in nanoseconds                        |
|     attr.*     |    *     | other attributes of a span                               |

#### Metrics

Metrics file contains a time series of metric values.

|    Column    |   Type   | Description                                         |
| :----------: | :------: | :-------------------------------------------------- |
|     time     | datetime | UTC timestamp of a metric value                     |
|    metric    |  string  | name of the metric value                            |
|    value     | float64  | value of the metric value                           |
| service_name |  string  | name of the service that generated the metric value |
|    attr.*    |    *     | other attributes of a metric value                  |

#### Logs

Logs file contains a time series of log events.

|    Column    |   Type   | Description                                      |
| :----------: | :------: | :----------------------------------------------- |
|     time     | datetime | UTC timestamp of a log event                     |
|   trace_id   |  string  | unique identifier of a trace                     |
|   span_id    |  string  | unique identifier of a span                      |
| service_name |  string  | name of the service that generated the log event |
|    level     |  string  | log level (e.g., INFO, ERROR)                    |
|   message    |  string  | log message                                      |
|    attr.*    |    *     | other attributes of a log event                  |
