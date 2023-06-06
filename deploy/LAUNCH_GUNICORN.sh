#!/bin/bash
# Date: 11-Aug-2020
# Deployment using gunicorn server
# Updates: James Smith 2023
# Run as:
#
#     nohup ./deploy/LAUNCH_GUNICORN.sh >& LOGTODAY
##

# find path to project root
THISFILE=`basename $0`
if [ $THISFILE != 'LAUNCH_GUNICORN.sh' ]
then
  echo "error in launch_gunicorn.sh - \$0 does not produce correct result on this system"
  exit 1
fi
HERE=`cd $(dirname $0) && pwd`
TOPDIR=`dirname $HERE`

# determine whether running container
if [ $TOPDIR == "/" ]
then
  # docker
  cd '/app'
else
  # non-docker
  cd $TOPDIR
fi
echo "HERE = $HERE"
echo "TOPDIR = $TOPDIR"

# write start time to log file for purposes of determining uptime status
UPTIME_START=`echo $(date +%s)`
echo "UPTIME_START = $UPTIME_START"
# for serverStatusRequest, Docker requires reading from root directory, so don't write to deploy folder
echo $UPTIME_START > $TOPDIR/uptime.txt

# determine whether running docker
DIR=$TOPDIR
if [ $TOPDIR == "/" ]
then
  DIR='/app'
fi

# read server vars from config file
CONFIG_FILE="$DIR/rcsb/app/config/config.yml"
SERVER_HOST_AND_PORT=`cat $CONFIG_FILE | grep SERVER_HOST_AND_PORT | sed 's/SERVER_HOST_AND_PORT://' | sed 's/http://' | sed 's/\///g' | sed 's/ //g'`
SURPLUS_PROCESSORS=`cat $CONFIG_FILE | grep SURPLUS_PROCESSORS | sed 's/SURPLUS_PROCESSORS://' | sed 's/ //g'`
PROCESSORS=`getconf _NPROCESSORS_ONLN`
WORKERS=$(( PROCESSORS - SURPLUS_PROCESSORS ))
if [ $WORKERS -lt 1 ]
then
  $WORKERS = 1
fi
echo "SERVER HOST AND PORT = $SERVER_HOST_AND_PORT"
echo "WORKERS = $WORKERS"

# start fastapi server
exec gunicorn \
rcsb.app.file.main:app \
    --chdir $TOPDIR \
    --bind $SERVER_HOST_AND_PORT \
    --timeout 300 \
    --reload \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers $WORKERS \
    --access-logfile - \
    --error-logfile - \
    --capture-output \
    --enable-stdio-inheritance
#
