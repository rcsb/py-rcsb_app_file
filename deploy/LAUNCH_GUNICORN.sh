#!/bin/bash
# Date: 11-Aug-2020
# Example deployment using gunicorn server
#
# Run as:
#
#     nohup ./deploy/LAUNCH_GUNICORN.sh >& LOGTODAY
##

THISFILE=`basename $0`
if [ $THISFILE != 'LAUNCH_GUNICORN.sh' ]
then
  echo "error in launch_gunicorn.sh - \$0 does not produce correct result on this system"
  exit 1
fi
HERE=`cd $(dirname $0) && pwd`
TOPDIR=`dirname $HERE`
if [ $TOPDIR == "/" ]
then
  cd '/app'
else
  cd $TOPDIR
fi
echo "HERE = $HERE"
echo "TOPDIR = $TOPDIR"
#
UPTIME_START=`echo $(date +%s)`
echo "UPTIME_START = $UPTIME_START"
# for ServerStatusRequest, Docker requires reading from root directory
echo $UPTIME_START > $TOPDIR/uptime.txt
#
DIR=$TOPDIR
if [ $TOPDIR == "/" ]
then
  DIR='/app'
fi
CONFIG_FILE="$DIR/rcsb/app/config/config.yml"
SERVER_HOST_AND_PORT=`cat $CONFIG_FILE | grep SERVER_HOST_AND_PORT | sed 's/SERVER_HOST_AND_PORT://' | sed 's/http://' | sed 's/\///g' | sed 's/ //g'`
PROCESSORS=`getconf _NPROCESSORS_ONLN`
WORKERS=$(( PROCESSORS - 1 ))
if [ $WORKERS -lt 1 ]
then
  $WORKERS = 1
fi
echo "SERVER HOST AND PORT = $SERVER_HOST_AND_PORT"
echo "WORKERS = $WORKERS"
#
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
