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
import asyncio
from fastapi import APIRouter, Form
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


@router.post("/asyncTest", status_code=200)
async def asyncTest(i: int = Form(1), w: int = Form(10)) -> dict:
    """

    Args:
        i: index of task
        w: wait time of task

    Returns:
        inputs - the point is to invoke inputs that will be returned out of order - refer to testAsync
    """
    logging.info("request from %d to sleep %d", i, w)
    await asyncio.sleep(w)
    return {"index": i, "time": w}
