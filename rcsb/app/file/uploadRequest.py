##
# File: uploadRequest.py
# Date: 10-Aug-2021
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os.path
from enum import Enum

from fastapi import APIRouter
from fastapi import Depends
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import UploadFile
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer

logger = logging.getLogger(__name__)

router = APIRouter()


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class UploadResult(BaseModel):
    fileName: str = Field(None, title="Stored file name", description="Stored file name", example="D_00000000.cif.V3")
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


class UploadSliceResult(BaseModel):
    sliceCount: str = Field(None, title="Slice count", description="Slice count", example="2")
    success: bool = Field(None, title="Success status", description="Success status", example="True")


@router.post("/upload", response_model=UploadResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def upload(
    uploadFile: UploadFile = File(...),
    idCode: str = Form(None, title="ID Code", description="Identifier code", example="D_00000000"),
    repositoryType: str = Form(None, title="Repository Type", description="OneDep repository type", example="deposit, archive"),
    contentType: str = Form(None, title="Content Type", description="OneDep content type", example="model"),
    partNumber: int = Form(None, title="Part Number", description="OneDep part number", example="1"),
    contentFormat: str = Form(None, title="Content format", description="Content format", example="cif, xml, json, txt"),
    version: str = Form(None, title="Version", description="OneDep version number of descriptor", example="1, 2, latest, next"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
    copyMode: str = Form("native", title="Copy mode", description="Copy mode", example="shell|native|gzip_decompress"),
    allowOverWrite: bool = Form(False, title="Allow overwrite of existing files", description="Allow overwrite of existing files", example="False"),
):
    fn = None
    try:
        cachePath = os.environ.get("CACHE_PATH", ".")
        cP = ConfigProvider(cachePath)
        #
        logger.debug("idCode %r hash %r hashType %r", idCode, hashDigest, hashType)
        #
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("uploadFile %s (%r)", fn, ct)
        #
        if fn.endswith(".gz") or ct == "application/gzip":
            copyMode = "gzip_decompress"
        #
        logger.debug("hashType.name %r hashDigest %r", hashType, hashDigest)
        ioU = IoUtils(cP)
        ret = await ioU.storeUpload(
            uploadFile.file,
            repositoryType,
            idCode,
            contentType,
            partNumber,
            contentFormat,
            version,
            allowOverWrite=allowOverWrite,
            copyMode=copyMode,
            hashType=hashType,
            hashDigest=hashDigest,
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {"success": False, "statusCode": 400, "statusMessage": "Upload fails with %s" % str(e)}
    #
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret


@router.post("/upload-slice", response_model=UploadSliceResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def uploadSlice(
    uploadFile: UploadFile = File(...),
    sliceIndex: int = Form(1, title="Index of the current chunk", description="Index of the current chunk", example="1"),
    sliceTotal: int = Form(1, title="Total number of chunks in the session", description="Total number of chunks in the session", example="5"),
    sessionId: str = Form(None, title="Session identifier", description="Unique identifier for the current session", example="9fe2c4e93f654fdbb24c02b15259716c"),
    copyMode: str = Form("native", title="Copy mode", description="Copy mode", example="shell|native|gzip_decompress"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
):
    ret = {}
    try:
        cachePath = os.environ.get("CACHE_PATH", ".")
        cP = ConfigProvider(cachePath)
        #
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("sliceIndex %d sliceTotal %d fn %r", sliceIndex, sliceTotal, fn)
        ioU = IoUtils(cP)
        ret = await ioU.storeSlice(uploadFile.file, sliceIndex, sliceTotal, sessionId, copyMode=copyMode, hashType=hashType, hashDigest=hashDigest)
        logger.debug("sliceIndex %d sliceTotal %d return %r", sliceIndex, sliceTotal, ret)
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {"success": False, "statusCode": 400, "statusMessage": "Slice upload fails with %s" % str(e)}

    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret


@router.post("/join-slice", response_model=UploadResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def joinUploadSlice(
    idCode: str = Form(None, title="ID Code", description="Identifier code", example="D_00000000"),
    repositoryType: str = Form(None, title="Repository Type", description="OneDep repository type", example="deposit, archive"),
    contentType: str = Form(None, title="Content Type", description="OneDep content type", example="model"),
    partNumber: int = Form(None, title="Part Number", description="OneDep part number", example="1"),
    contentFormat: str = Form(None, title="Content format", description="Content format", example="cif, xml, json, txt"),
    version: str = Form(None, title="Version", description="OneDep version number of descriptor", example="1, 2, latest, next"),
    sliceTotal: int = Form(1, title="Total number of chunks in the session", description="Total number of chunks in the session", example="5"),
    sessionId: str = Form(None, title="Session identifier", description="Unique identifier for the current session", example="9fe2c4e93f654fdbb24c02b15259716c"),
    copyMode: str = Form("native", title="Copy mode", description="Copy mode", example="shell|native|gzip_decompress"),
    allowOverWrite: bool = Form(False, title="Allow overwrite of existing files", description="Allow overwrite of existing files", example="False"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
):
    ret = {}
    try:
        cachePath = os.environ.get("CACHE_PATH", ".")
        cP = ConfigProvider(cachePath)
        #
        logger.debug("sliceTotal %d", sliceTotal)
        # ---
        ioU = IoUtils(cP)
        ret = await ioU.finalizeMultiSliceUpload(
            sliceTotal,
            sessionId,
            repositoryType,
            idCode,
            contentType,
            partNumber,
            contentFormat,
            version,
            allowOverWrite,
            copyMode,
            hashType=hashType,
            hashDigest=hashDigest,
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", idCode, sliceTotal, str(e))
        ret = {"success": False, "statusCode": 400, "statusMessage": "Slice upload fails with %s" % str(e)}

    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret
