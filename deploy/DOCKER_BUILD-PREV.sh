#!/usr/bin/env bash
#
# File: DOCKER_BUILD.sh
# Date: 22-Aug-2020 jdw
#
# To build the container user this command -
#    REGISTRY_USER="githubuser" ./deploy/DOCKER_BUILD.sh
#

REGISTRY_USER=${REGISTRY_USER}
echo "Using registry user name: ${REGISTRY_USER}"
#
IMAGE_NAME=${IMAGE_NAME:-"py-rcsb_app_template"}
TAG_TEST=${TAG_TEST:-"unittest"}
TAG_BASE=${TAG_BASE:-"test"}
PY_MODULE=${PY_MODULE:-"template"}
#
VER=`grep '__version__' rcsb/app/${PY_MODULE}/__init__.py | awk '{print $3}' | tr -d '"'`
#
docker build --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` \
             --build-arg VCS_REF=`git rev-parse --short HEAD` \
             --build-arg VERSION=$VER \
             --tag ${REGISTRY_USER}/${IMAGE_NAME}:${TAG_TEST} \
             --file ./Dockerfile.${TAG_BASE} .
#
#
docker image ls
docker container ls
# -
rm -f ${IMAGE_NAME}-docker-run-tox.log
docker rm -f ${IMAGE_NAME}

#
echo "++Begin running unittests on image ${REGISTRY_USER}/${IMAGE_NAME}:${TAG_TEST}"
docker run --rm --name ${IMAGE_NAME} ${REGISTRY_USER}/${IMAGE_NAME}:${TAG_TEST} tox >& ${IMAGE_NAME}-docker-run-tox.log || {
    echo "++Unittests failing for ${IMAGE_NAME} ---- return code: $?"
    docker rmi -f ${IMAGE_NAME}
    docker rm -f ${REGISTRY_USER}/${IMAGE_NAME}:${TAG_TEST}
    exit 1
}
echo "++Unit Tests succeed for ${IMAGE_NAME} ---- return code: $?"
#
docker image ls
docker rmi -f ${REGISTRY_USER}/${IMAGE_NAME}:${TAG_TEST}
#
docker build --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` \
             --build-arg VCS_REF=`git rev-parse --short HEAD` \
             --build-arg VERSION=$VER \
             --tag ${REGISTRY_USER}/${IMAGE_NAME}:${TAG_BASE} \
             --tag ${REGISTRY_USER}/${IMAGE_NAME}:${VER} \
             --file ./Dockerfile  .
#
docker rmi -f $(docker images -f "dangling=true" -q)
docker image ls
#
# docker push  ${REGISTRY_USER}/${IMAGE_NAME}
