#! /usr/bin/env sh
#
# File: launch.sh
# Date: 22-Aug-2020
#
# Docker CMD main entry point script.
##
set -e

# App module template covention is - rcsb.app.<service_name>.main:app
SERVICE_NAME=${SERVICE_NAME:-"file"}
export APP_MODULE="rcsb.app.${SERVICE_NAME}.main:app"
export GUNICORN_CONF=${GUNICORN_CONF:-"/app/gunicorn_conf.py"}
export WORKER_CLASS=${WORKER_CLASS:-"uvicorn.workers.UvicornWorker"}

THISFILE=`basename $0`
if [ $THISFILE != 'launch.sh' ]
then
  echo "error in launch.sh - \$0 does not produce correct result on this system"
  exit 1
fi
HERE=`cd $(dirname $0); pwd`
TOPDIR=`dirname $HERE`
echo "HERE=$HERE"
echo "TOPDIR=$TOPDIR"
UPTIME_START=`echo $(date +%s)`
echo "UPTIME_START=$UPTIME_START"
echo $UPTIME_START > uptime.txt

# Optional setup.sh
SETUP_PATH=${SETUP_PATH:-/app/setup.sh}
echo "Checking for setup script in $SETUP_PATH"
if [ -f $SETUP_PATH ] ; then
    echo "Running setup script $SETUP_PATH"
    . "$SETUP_PATH"
else
    echo "There is no setup script $SETUP_PATH"
fi

# Start Gunicorn
echo "Worker class is $WORKER_CLASS"
echo "Gunicorn config is $GUNICORN_CONF"
echo "Application module is $APP_MODULE"
#
cd /app
exec gunicorn -k "$WORKER_CLASS" -c "$GUNICORN_CONF" "$APP_MODULE"
