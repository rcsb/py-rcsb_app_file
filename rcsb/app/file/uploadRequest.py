##
# File: uploadRequest.py
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
    filePath: str = Field(
        None, title="file path", description="relative file path", example="deposit/D_000_model_P1.cif.V1"
    )
    chunkIndex: int = Field(
        None, title="chunk index", description="chunk index", example=0
    )
    uploadId: str = Field(
        None, title="upload id", description="upload id", example="A1101A10101E"
    )


@router.get("/getUploadParameters", response_model=UploadResult)
async def getUploadParameters(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: Optional[str] = Query(default="next"),
    partNumber: int = Query(...),
    contentFormat: str = Query(...),
    version: str = Query(default="next"),
    allowOverwrite: bool = Query(default=True),
    resumable: bool = Query(default=False),
):
    ret = None
    try:
        ret = await UploadUtility().getUploadParameters(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            allowOverwrite,
            resumable,
        )
    except HTTPException as exc:
        # ret = {
        #     "success": False,
        #     "statusCode": exc.status_code,
        #     "statsMessage": f"error in upload parameters {exc.detail}",
        # }
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    return ret


# upload chunked file
@router.post("/upload")
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
        up = UploadUtility()
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
    up = UploadUtility()
    return await up.clearSession(uploadIds, None)


# purge kv before testing
@router.post("/clearKv")
async def clearKv():
    up = UploadUtility()
    return await up.clearKv()
