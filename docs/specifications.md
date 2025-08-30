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

## Trace Sampler Specification

An instance of a trace sampler class is initialized with its configuration (hyperparameters).

The instance takes sampler arguments and returns a list of sample results.

The sampler arguments contain
+ dataset name
+ datapack name
+ input directory containing the data files of the datapack
+ output directory for storing intermediate results
+ sampling rate (float between 0.0 and 1.0)
+ sampling mode (online or offline)

The sample result contains a trace_id and its sample_score (weight for sampling).

### Sampling Modes

+ **Online Mode**: Returns all traces with their sampling scores, no limit on count
+ **Offline Mode**: Limited by sampling rate, sorts traces by score and keeps top traces

### Performance Metrics

Sampler performance is evaluated using the following metrics:

+ **Controllability (RoD)**: Rate of Deviation = $|((N_s - N_e) / N_e)|$
  - $N_s$: actual sampled count
  - $N_e$: expected sampled count
+ **API Coverage**: API Coverage Rate = $N_t' / N_t$
  - $N_t$': number of sampled trace types (entry spans)
  - $N_t$: total number of trace types
+ **Path Coverage**: Execution Path Coverage Rate = $N_p' / N_p$
  - $N_p$': number of sampled execution path types
  - $N_p$: total number of execution path types
  - Uses BFS traversal with sorted nodes at same depth for consistent encoding
+ **Event Coverage**: Event Coverage Rate = $N_e' / N_e$
  - $N_e$': number of sampled event pairs (2-grams)
  - $N_e$: total number of event pairs
  - Events are encoded from traces and logs combined
  - Includes span events, status errors, performance degradation, and log events
+ **Proportion (PRO)**: Three proportion metrics
  - PRO_anomaly: proportion of detector-flagged spans in abnormal traces only
  - PRO_rare: proportion of rare entry spans sampled (< 5% frequency)
  - PRO_common: proportion of common spans (including detector spans in normal traces)
+ **Runtime**: Algorithm runtime per span in milliseconds
+ **Actual Sampling Rate**: Achieved sampling rate

#### Coverage Metrics Comparison

- **API Coverage**: Simple coverage based on entry span names (API endpoints)
  - Fast to calculate and good for basic assessment
- **Path Coverage**: Advanced coverage based on complete execution paths using TracePicker-style encoding
  - Handles parallel calls by sorting at same depth
  - Provides more detailed insight into trace structure diversity
  - Generally more strict than API coverage as multiple paths can share the same entry point
- **Event Coverage**: Most granular coverage based on event sequences from traces and logs
  - Encodes traces and logs into events (spans, errors, performance issues, log entries)
  - Calculates coverage using consecutive event pairs (2-grams)
  - Considers performance degradation using metrics_sli.parquet thresholds
  - Provides the most comprehensive view of system behavior patterns
  - Generally the most strict coverage metric, showing lowest percentages

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
