#!/bin/bash -ex
IMAGE_PREFIX="10.10.10.240/library"
IMAGE_NAME="clickhouse_dataset"
COMMIT_HASH=$(git rev-parse --short HEAD)
IMAGE_FULL="${IMAGE_PREFIX}/${IMAGE_NAME}:${COMMIT_HASH}"

function build() {
    # assert that the working directory is clean
    [[ -z "$(git status -s)" ]] # https://stackoverflow.com/a/9393642

    docker build \
        --network=host \
        --build-arg HTTP_PROXY="${HTTP_PROXY}" \
        --build-arg HTTPS_PROXY="${HTTPS_PROXY}" \
        -t ${IMAGE_FULL} \
        -f ./Dockerfile .
}

function push() {
    docker push ${IMAGE_FULL}
    docker tag ${IMAGE_FULL} "${IMAGE_PREFIX}/${IMAGE_NAME}:latest"
    docker push "${IMAGE_PREFIX}/${IMAGE_NAME}:latest"
}

case $1 in
    build)
        build
        ;;
    push)
        push
        ;;
    *)
        echo "Usage: $0 {build|push}"
        exit 1
        ;;
esac
