

# Notes of Building Dataset

```bash
# patch detection result, convert the dataset to standard RCABench format in */converted directory
sudo -E ./cli/detector.py patch-detection

# copy the converted dataset to the rcabench-platform-v2
./cli/dataset_transform/make_rcabench.py run

# do some filtering strategies
./cli/dataset_transform/make_rcabench_filtered.py run
```