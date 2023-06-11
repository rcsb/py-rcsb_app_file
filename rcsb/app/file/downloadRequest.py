##
# File: downloadRequest.py
# Date: 11-Aug-2021
# Updates: James Smith 2023

##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

"""
    Download a single file
    Download/upload a session bundle
"""

import logging
import typing
from enum import Enum
from fastapi import APIRouter, HTTPException
from fastapi import Query
from rcsb.app.file.DownloadUtility import DownloadUtility

logger = logging.getLogger(__name__)

# not possible to secure an HTML form with a JWT, so no dependencies
router = APIRouter(tags=["download"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


@router.get("/download")
async def download(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: str = Query(default=""),
    partNumber: int = Query(default=1),
    contentFormat: str = Query(...),
    version: str = Query(default="latest"),
    hashType: HashType = Query(...),
    chunkSize: typing.Optional[int] = None,
    chunkIndex: typing.Optional[int] = None,
):
    try:
        return await DownloadUtility().download(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            hashType,
            chunkSize,
            chunkIndex,
        )
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
