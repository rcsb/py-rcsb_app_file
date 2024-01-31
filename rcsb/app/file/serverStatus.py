##
# File: serverStatus.py
# Date: 11-Aug-2020
# Updates: James Smith 2023
##

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
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.ProcessStatusUtil import ProcessStatusUtil

logger = logging.getLogger(__name__)


class ServerStatus:
    @staticmethod
    def serverStatus():
        # status of gunicorn server and remote repository file system
        status = {"server running": True}
        uptime = ServerStatus.getUptime()
        status.update(uptime)
        storage = ServerStatus.getServerStorage()
        status.update(storage)
        # commented out for Azure tox tests which don't have redis
        # red = getRedisStatus()
        # status.update(red)
        return status

    @staticmethod
    def getUptime():
        HERE = os.path.dirname(__file__)
        TOPDIR = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(HERE))))
        uptime_file = os.path.join(TOPDIR, "uptime.txt")
        uptime_start = 0
        with open(uptime_file, "r", encoding="UTF-8") as read:
            uptime_start = float(read.read())
        uptime_stop = time.time()
        seconds = uptime_stop - uptime_start
        minutes = seconds / 60
        hours = minutes / 60
        days = minutes / 24
        # report total uptime in either hours, minutes, or seconds (i.e. total hours, or total minutes, or total seconds)
        return {
            "uptime days total": int(days),
            "uptime hours total": int(hours),
            "uptime minutes total": int(minutes),
            "uptime seconds total": int(seconds),
            "uptime start": int(uptime_start),
            "uptime now": int(uptime_stop),
        }

    @staticmethod
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
        return {"redis running": result}

    @staticmethod
    def getServerStorage():
        percent_ram_used = psutil.virtual_memory()[2]
        cP = ConfigProvider()
        repository_dir_path = cP.get("REPOSITORY_DIR_PATH")
        disk_usage = shutil.disk_usage(repository_dir_path)
        disk_total = disk_usage[0]
        disk_used = disk_usage[1]
        disk_free = disk_usage[2]
        percent_disk_used = (disk_used / disk_total) * 100
        return {
            "server percent ram used": percent_ram_used,
            "repository percent disk used": percent_disk_used,
            "repository disk bytes total": disk_total,
            "repository disk bytes used": disk_used,
            "repository disk bytes free": disk_free,
        }

    @staticmethod
    def processStatus():
        # status of machine that server is on
        cP = ConfigProvider()
        psU = ProcessStatusUtil()
        psD = psU.getInfo()
        return {"msg": "Status is nominal!", "version": cP.getVersion(), "status": psD}
