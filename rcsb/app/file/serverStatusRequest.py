##
# File: serverStatusRequest.py
# Date: 11-Aug-2020
#
##
# pylint: skip-file

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import time
import redis
import psutil
import shutil
from fastapi import APIRouter
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.ProcessStatusUtil import ProcessStatusUtil

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/uptime", tags=["status"])
def getUptime():
    global TOPDIR
    HERE = os.path.dirname(__file__)
    TOPDIR = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(HERE))))
    uptime_file = os.path.join(TOPDIR, "uptime.txt")
    uptime_start = 0
    with open(uptime_file, "r") as read:
        uptime_start = float(read.read())
    uptime_stop = time.time()
    seconds = uptime_stop - uptime_start
    minutes = seconds / 60
    hours = minutes / 60
    days = minutes / 24
    # report total uptime in either hours, minutes, or seconds (i.e. total hours, or total minutes, or total seconds)
    return {"days_total": int(days), "hours_total": int(hours), "minutes_total": int(minutes), "seconds_total": int(seconds), "start": int(uptime_start), "stop": int(uptime_stop)}


@router.get("/redis-status", tags=["status"])
def getRedisStatus():
    # create database if not exists
    # create table if not exists
    cP = ConfigProvider()
    redis_host = cP.get("REDIS_HOST")
    try:
        r = redis.Redis(host=redis_host, decode_responses=True)
    except Exception as exc:
        # already exists
        logging.warning("exception in redis status: %s %s", type(exc), exc)
    try:
        result = r.ping() == True  # noqa: E712
    except Exception:
        result = False
    return {"running": result}


@router.get("/storage", tags=["status"])
def getServerStorage():
    percent_ram_used = psutil.virtual_memory()[2]
    cP = ConfigProvider()
    repository_dir_path = cP.get("REPOSITORY_DIR_PATH")
    disk_usage = shutil.disk_usage(repository_dir_path)
    disk_total = disk_usage[0]
    disk_used = disk_usage[1]
    disk_free = disk_usage[2]
    percent_disk_used = (disk_used / disk_total) * 100
    return {"percent_ram_used": percent_ram_used, "percent_disk_used": percent_disk_used, "total": disk_total, "used": disk_used, "free": disk_free}


@router.get("/status", tags=["status"])
def serverStatus():
    cP = ConfigProvider()
    psU = ProcessStatusUtil()
    psD = psU.getInfo()
    return {"msg": "Status is nominal!", "version": cP.getVersion(), "status": psD}


@router.get("/", tags=["status"])
def rootServerStatus():
    return {"msg": "Service is up!"}


@router.get("/healthcheck", tags=["status"])
def rootHealthCheck():
    return "UP"
