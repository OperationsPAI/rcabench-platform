#!/bin/bash -ex
juicefs format \
    --storage minio \
    --bucket http://localhost:9000/juicefs \
    --access-key minioadmin \
    --secret-key minioadmin \
    redis://localhost:6379/1 \
    jfs1
