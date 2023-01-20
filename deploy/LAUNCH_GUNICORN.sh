#!/bin/bash
# Date: 11-Aug-2020
# Example deployment using gunicorn server
#
# Run as:
#
#     nohup ./deploy/LAUNCH_GUNICORN.sh >& LOGTODAY
##
HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TOPDIR="$(dirname "$HERE")"
echo "HERE=${HERE}"
echo "TOPDIR=${TOPDIR}"
#

THISIP=${HOSTIP:="127.0.0.1"}
THISPORT=${HOSTPORT:="8000"}
ADDR=${THISIP}:${THISPORT}
#
cd ${TOPDIR}
gunicorn \
rcsb.app.file.main:app \
    --timeout 300 \
    --chdir ${TOPDIR} \
    --bind ${ADDR} \
    --reload \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 5 \
    --access-logfile - \
    --error-logfile - \
    --capture-output \
    --enable-stdio-inheritance
#
