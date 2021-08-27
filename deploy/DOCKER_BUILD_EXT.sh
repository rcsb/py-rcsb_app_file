#!/usr/bin/env bash
#
# File: DOCKER_BUILD_EXT.sh
# Date:  7-Aug-2021 jdw
#       Build deployable application container including dependencies for mmCIF source build
#
# To build the container user this command -
#    REGISTRY_USER="githubuser" ./deploy/DOCKER_BUILD_EXT.sh
#

REGISTRY_USER=${REGISTRY_USER}
#
PACKAGE_NAME=${PACKAGE_NAME:-"py-rcsb_app_file"}
TAG_BASE=${TAG_BASE:-"devel"}
DOCKER_FILE_PATH="./Dockerfile.${TAG_BASE}"
PY_MODULE=${PY_MODULE:-"file"}
VER=`grep '__version__' rcsb/app/${PY_MODULE}/__init__.py | awk '{print $3}' | tr -d '"'`
IMAGE_NAME_1=${REGISTRY_USER}/${PACKAGE_NAME}:${VER}
IMAGE_NAME_2=${REGISTRY_USER}/${PACKAGE_NAME}:${TAG_BASE}
CONTAINER_NAME="cnt-${PACKAGE_NAME}-${TAG_BASE}-check"
#
echo "++ Building container for ${PACKAGE_NAME} version ${VER} source (${DOCKER_FILE_PATH}) registry user ${REGISTRY_USER}"
#
docker build --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` \
             --build-arg VCS_REF=`git rev-parse --short HEAD` \
             --build-arg VERSION=$VER \
             --build-arg USER_ID=$(id -u) \
             --build-arg GROUP_ID=$(id -g) \
             --tag ${IMAGE_NAME_1} \
             --tag ${IMAGE_NAME_2} \
             --file ${DOCKER_FILE_PATH} .
#
#docker scan ${IMAGE_NAME_1}
#
echo "++ Sanity check image"
docker rm -f ${CONTAINER_NAME}
docker run  --name ${CONTAINER_NAME}  --detach --publish 80:80 ${IMAGE_NAME_1} check
docker logs --details ${CONTAINER_NAME}
#
echo "++ Cleanup containers and temporary images"
ec=$(docker images -f "dangling=true" -q)
if [ ! -z "$ec" ]
then
    docker rmi -f $(docker images -f "dangling=true" -q)
fi
docker rm -f ${CONTAINER_NAME}
#

