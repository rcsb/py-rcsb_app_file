##
# File: UploadRequest.py
# Date: 27-Oct-2022
#
##
__docformat__ = "google en"
__author__ = "James Smith, Ahsan Tanweer"
__email__ = "james.smith@rcsb.org, ahsan@ebi.ac.uk"
__license__ = "Apache 2.0"


import logging
from enum import Enum
from typing import Optional
from fastapi import APIRouter, Query, File, Form, HTTPException, UploadFile, Depends
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.UploadUtility import UploadUtility
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer

logger = logging.getLogger(__name__)


provider = ConfigProvider()
jwtDisable = bool(provider.get("JWT_DISABLE"))
if not jwtDisable:
    router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
else:
    router = APIRouter(tags=["upload"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class UploadResult(BaseModel):
    success: bool = Field(
        None, title="Success status", description="Success status", example=True
    )
    statusCode: int = Field(
        None, title="HTTP status code", description="HTTP status code", example=200
    )
    statusMessage: str = Field(
        None, title="Status message", description="Status message", example="Success"
    )


@router.get("/getUploadParameters")
async def getUploadParameters(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: Optional[str] = Query(default="next"),
    partNumber: int = Query(...),
    contentFormat: str = Query(...),
    version: str = Query(default="next"),
    allowOverwrite: bool = Query(default=True),
    # hashDigest: str = Query(default=None),
    resumable: bool = Query(default=False),
):
    ret = None
    try:
        ret = await UploadUtility(ConfigProvider()).getUploadParameters(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            allowOverwrite,
            # hashDigest,
            resumable,
        )
    except HTTPException as exc:
        ret = {
            "success": False,
            "statusCode": exc.status_code,
            "statsMessage": f"error in upload parameters {exc.detail}",
        }
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    return ret


# upload chunked file
@router.post("/upload", response_model=UploadResult)
async def upload(
    # chunk parameters
    chunk: UploadFile = File(...),
    chunkSize: int = Form(None),
    chunkIndex: int = Form(None),
    expectedChunks: int = Form(None),
    # upload file parameters
    uploadId: str = Form(None),
    hashType: str = Form(None),
    hashDigest: str = Form(None),
    # save file parameters
    filePath: str = Form(...),
    decompress: bool = Form(False),
    allowOverwrite: bool = Form(False),
    # other
    resumable: bool = Form(False),
):
    ret = None
    try:
        cP = ConfigProvider()
        up = UploadUtility(cP)
        ret = await up.upload(
            # chunk parameters
            chunk=chunk.file,
            chunkSize=chunkSize,
            chunkIndex=chunkIndex,
            expectedChunks=expectedChunks,
            # upload file parameters
            uploadId=uploadId,
            hashType=hashType,
            hashDigest=hashDigest,
            # save file parameters
            filePath=filePath,
            decompress=decompress,
            allowOverwrite=allowOverwrite,
            resumable=resumable,
        )
    except HTTPException as exc:
        ret = {
            "success": False,
            "statusCode": exc.status_code,
            "statusMessage": f"error in upload {exc.detail}",
        }
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    return ret


# clear kv entries from one user
@router.post("/clearSession")
async def clearSession(uploadIds: list = Form(...)):
    cP = ConfigProvider()
    up = UploadUtility(cP)
    return await up.clearSession(uploadIds, None)


# purge kv before testing
@router.post("/clearKv")
async def clearKv():
    cP = ConfigProvider()
    up = UploadUtility(cP)
    return await up.clearKv()
