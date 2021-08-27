#!/usr/bin/env bash
#
# File: DOCKER_UNITTEST.sh
# Date:  7-Aug-2021 jdw
#       Build a container, run unittests using tox, and cleanup.
#
# To build the container user this command -
#    REGISTRY_USER="githubuser" ./deploy/DOCKER_UNITTEST.sh
#

REGISTRY_USER=${REGISTRY_USER}
#
PACKAGE_NAME=${PACKAGE_NAME:-"py-rcsb_app_file"}
TAG_BASE=${TAG_BASE:-"unittest"}
DOCKER_FILE_PATH="./Dockerfile.${TAG_BASE}"
PY_MODULE=${PY_MODULE:-"file"}
VER=`grep '__version__' rcsb/app/${PY_MODULE}/__init__.py | awk '{print $3}' | tr -d '"'`
IMAGE_NAME=${REGISTRY_USER}/${PACKAGE_NAME}:${TAG_BASE}
CONTAINER_NAME="Container-${PACKAGE_NAME}"
#
echo "++Building container for ${PACKAGE_NAME} version ${VER} source (${DOCKER_FILE_PATH}) registry user ${REGISTRY_USER}"
#
docker build --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` \
             --build-arg VCS_REF=`git rev-parse --short HEAD` \
             --build-arg VERSION=$VER \
             --build-arg USER_ID=$(id -u) \
             --build-arg GROUP_ID=$(id -g) \
             --tag ${IMAGE_NAME} \
             --file ${DOCKER_FILE_PATH} .
#
#
#docker image ls
#docker container ls
# -
rm -f ${PACKAGE_NAME}-docker-run-tox.log
#
echo "++Begin running unittests on image ${IMAGE_NAME}"
docker run --rm --name ${PACKAGE_NAME} ${IMAGE_NAME} tox >& ${PACKAGE_NAME}-docker-run-tox.log || {
    echo "++Unittests failing for container ${CONTAINER_NAME} ---- return code: $?"
    docker rmi -f ${IMAGE_NAME}
    exit 1
}
echo "++Unit Tests succeed for container ${CONTAINER_NAME} ---- return code: $?"
#
docker rmi -f ${IMAGE_NAME}
#docker image ls
#docker container ls
#
#