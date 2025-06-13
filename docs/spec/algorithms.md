# RCA Algorithm Specification

An instance of an RCA algorithm class is initialized with its configuration (hyperparameters).

The instance takes arguments and returns a list of answers.

The algorithm arguments contain
+ dataset name
+ datapack name
+ input directory containing the data files of the datapack
+ output directory for storing intermediate results

The algorithm answer is a predicted root cause with level, name and rank.
