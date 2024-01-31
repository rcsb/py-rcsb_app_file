##
# File: serverStatusRequest.py
# Date: 11-Aug-2020
# Updates: James Smith 2023
##

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import logging
from fastapi import APIRouter
from rcsb.app.file.serverStatus import ServerStatus

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", tags=["status"])
def root():
    return ServerStatus.serverStatus()


@router.get("/status", tags=["status"])
def serverStatus():
    return ServerStatus.serverStatus()


@router.get("/processStatus", tags=["status"])
def processStatus():
    return ServerStatus.processStatus()
