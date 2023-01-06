##
# File: uploadRequest.py
# Date: 27-Oct-2022
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import gzip
import hashlib
import io
import logging
import os
import re
import shutil
from enum import Enum
from typing import Optional
from fastapi import APIRouter, Path, Query, Request, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.FileLock import FileLock
# import json
# import pydantic
# from pydantic import ValidationError
# from rcsb.app.file.PathUtils import PathUtils
# from rcsb.utils.io.FileLock import FileLock
# from rcsb.app.file.pathRequest import latestFileVersion

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["upload"])


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
    uploadId: str = Field(
        None, title="uploadId", description="uploadId", example="asdf4as56df4657sd4f57"
    )
    fileName: str = Field(
        None, title="fileName", description="fileName", example="D_10000_model_P1.cif.V1"
    )


# return kv entry from file parameters, or None
@router.get("/uploadStatus")
async def getUploadStatus(repositoryType: str = Query(...),
                          depId: str = Query(...),
                          contentType: str = Query(...),
                          milestone: Optional[str] = Query(default="next"),
                          partNumber: int = Query(...),
                          contentFormat: str = Query(...),
                          version: str = Query(default="next"),
                          hashDigest: str = Query(default=None)):
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    # logging.warning(f"upload status version {version} hash {hashDigest}")
    if version is None or not re.match(r"\d+", version):
        version = await ioU.findVersion(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version
        )
        # version = await latestFileVersion(depId, repositoryType, contentType, contentFormat, partNumber, milestone)
    # logging.warning(f"upload status version {version} hash {hashDigest}")
    uploadId = await ioU.getResumedUpload(
        repositoryType=repositoryType,
        depId=depId,
        contentType=contentType,
        milestone=milestone,
        partNumber=partNumber,
        contentFormat=contentFormat,
        version=version,
        hashDigest=hashDigest
    )
    if not uploadId:
        return None
    status = await ioU.getSession(uploadId)
    return status


# return kv entry from upload id
@router.get("/uploadStatusFromId/{uploadId}")
async def getUploadStatusFromId(uploadId: str = Path(...)):
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    status = await ioU.getSession(uploadId)
    return status


# find upload id from file parameters
@router.post("/findUploadId")
async def findUploadId(repositoryType: str = Form(),
                       depId: str = Form(...),
                       contentType: str = Form(...),
                       milestone: str = Form(...),
                       partNumber: int = Form(...),
                       contentFormat: str = Form(...),
                       version: str = Form(...)):
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    return await ioU.getResumedUpload(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)


# create new id
@router.get("/getNewUploadId")
async def getNewUploadId() -> str:
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    return await ioU.getNewUploadId()


# clear kv entries from one user
@router.post("/clearSession")
async def clearSession(uploadIds: list = Form(...)):
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    return await ioU.clearSession(uploadIds)


# purge kv before testing
@router.post("/clearKv")
async def clearKv():
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    return await ioU.clearKv()


@router.post("/upload", response_model=UploadResult)
async def upload(
    # upload file parameters
    uploadFile: UploadFile = File(...),
    uploadId: str = Form(None),
    hashType: HashType = Form(None),
    hashDigest: str = Form(None),
    copyMode: str = Form("native", example="shell|native|gzip_decompress"),
    # chunk parameters
    chunkSize: int = Form(None),
    chunkIndex: int = Form(0),
    chunkOffset: int = Form(0),
    expectedChunks: int = Form(1),
    chunkMode: str = Form("sequential", example="sequential, async"),
    # save file parameters
    depId: str = Form(None, example="D_1000000000"),
    repositoryType: str = Form(None, example="onedep-archive, onedep-deposit"),
    contentType: str = Form(None, example="model, structure-factors, val-report-full"),
    milestone: str = Form(None, example="release"),
    partNumber: int = Form(None),
    contentFormat: str = Form(None, example="pdb, pdbx, mtz, pdf"),
    version: str = Form(None, example="1, 2, latest, next"),
    allowOverWrite: bool = Form(False)
):
    fn = None
    ct = None
    try:
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        #
        logger.debug("depId %r hash %r hashType %r", depId, hashDigest, hashType)
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
            # upload file parameters
            ifh=uploadFile.file,
            uploadId=uploadId,
            hashType=hashType,
            hashDigest=hashDigest,
            copyMode=copyMode,
            # chunk parameters
            chunkIndex=chunkIndex,
            chunkOffset=chunkOffset,
            expectedChunks=expectedChunks,
            chunkMode=chunkMode,
            # save file parameters
            depId=depId,
            repositoryType=repositoryType,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
            allowOverWrite=allowOverWrite
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {
            "success": False,
            "statusCode": 400,
            "statusMessage": f"Upload fails with {str(e)}",
        }
    #
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret


@router.post("/expedite")
async def expediteUpload(
    request: Request,
    # upload file parameters
    uploadFile: UploadFile = File(...),
    uploadId: str = Form(None),
    hashType: HashType = Form(None),
    hashDigest: str = Form(None),
    copyMode: str = Form("native", example="shell|native|gzip_decompress"),
    # save file parameters
    depId: str = Form(None, example="D_1000000000"),
    repositoryType: str = Form(None, example="onedep-archive, onedep-deposit"),
    contentType: str = Form(None, example="model, structure-factors, val-report-full"),
    milestone: str = Form(None, example="release"),
    partNumber: int = Form(None),
    contentFormat: str = Form(None, example="pdb, pdbx, mtz, pdf"),
    version: str = Form(None, example="1, 2, latest, next"),
    allowOverWrite: bool = Form(False)
):
    tempPath = None
    outPath = None
    try:
        ifh = uploadFile.file
        fn = uploadFile.filename
        ct = uploadFile.content_type
        if fn.endswith(".gz") or ct == "application/gzip":
            copyMode = "gzip_decompress"
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        if not pathU.checkContentTypeFormat(contentType, contentFormat):
            return {"success": False, "statusCode": 405,
                    "statusMessage": "Bad content type and/or format - upload rejected"}
        lockPath = pathU.getFileLockPath(depId, contentType, milestone, partNumber, contentFormat)
        with FileLock(lockPath):
            outPath = pathU.getVersionedPath(
                repositoryType=repositoryType,
                depId=depId,
                contentType=contentType,
                milestone=milestone,
                partNumber=partNumber,
                contentFormat=contentFormat,
                version=version
            )
            if not outPath:
                return {"success": False, "statusCode": 405,
                        "statusMessage": "Bad content type metadata - cannot build a valid path"}
            if os.path.exists(outPath) and not allowOverWrite:
                return {"success": False, "statusCode": 405,
                        "statusMessage": "Encountered existing file - overwrite prohibited"}
        dirPath, fn = os.path.split(outPath)
        tempPath = os.path.join(dirPath, "." + fn)
        os.makedirs(dirPath, mode=0o755, exist_ok=True)
        if copyMode == "shell" or copyMode == "gzip_decompress":
            with open(tempPath, "wb") as ofh:
                shutil.copyfileobj(ifh, ofh)
        elif copyMode == "native":  # gzip throws error
            with open(tempPath, "wb") as ofh:
                async for chunk in request.stream():
                    ofh.write(chunk)
    except Exception as e:
        ret = {"success": False, "statusCode": 400, "statusMessage": f"Store fails with {str(e)}"}
    finally:
        ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
        if hashDigest and hashType:
            if hashType == "SHA1":
                hashObj = hashlib.sha1()
            elif hashType == "SHA256":
                hashObj = hashlib.sha256()
            elif hashType == "MD5":
                hashObj = hashlib.md5()
            blockSize = 65536
            with open(tempPath, "rb") as r:
                while chunk := r.read(blockSize):
                    hashObj.update(chunk)
            ok = hashObj.hexdigest() == hashDigest
            if not ok:
                ret = {"success": False, "statusCode": 400, "statusMessage": f"{hashType} hash check failed"}
            if ok:
                os.replace(tempPath, outPath)
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            if copyMode == "gzip_decompress":
                with gzip.open(outPath, "rb") as r:
                    with open(tempPath, "wb") as w:
                        w.write(r.read())
                os.replace(tempPath, outPath)
        else:
            ret = {"success": False, "statusCode": 500, "statusMessage": "Error - missing hash"}
        ifh.close()
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    return ret



