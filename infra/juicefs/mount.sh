#!/bin/bash -ex
sudo juicefs mount redis://redis.example.org:6379/1 /mnt/jfs -d
