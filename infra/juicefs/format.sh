#!/bin/bash -ex
juicefs format \
    --storage minio \
    --bucket http://minio.example.org:9000/juicefs \
    --access-key minioadmin \
    --secret-key minioadmin \
    redis://redis.example.org:6379/1 \
    jfs
