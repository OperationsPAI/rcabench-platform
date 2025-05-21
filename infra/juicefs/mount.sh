#!/bin/bash -ex
sudo juicefs mount redis://localhost:6379/1 /mnt/jfs1 -d
