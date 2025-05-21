#!/bin/bash -ex
juicefs format \
    --storage minio \
    --bucket http://10.10.10.38:9000/juicefs \
    --access-key minioadmin \
    --secret-key minioadmin \
    redis://10.10.10.38:6379/1 \
    jfs
