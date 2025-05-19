#!/bin/bash -ex
IMAGE_PREFIX="10.10.10.240/library"
IMAGE_NAME="rcabench-platform"
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

function run() {
    docker run \
        --network=host \
        -e HTTP_PROXY="${HTTP_PROXY}" \
        -e HTTPS_PROXY="${HTTPS_PROXY}" \
        -v ${PWD}/data:/app/data \
        -v ${PWD}/temp:/app/temp \
        -v ${PWD}/logs:/app/logs \
        -v ${PWD}/output:/app/output \
        -it ${IMAGE_FULL} \
        /bin/bash
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
    run)
        run
        ;;
    push)
        push
        ;;
    *)
        echo "Usage: $0 {build|run|push}"
        exit 1
        ;;
esac
