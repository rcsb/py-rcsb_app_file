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
import uuid
import json
from enum import Enum
from typing import Optional
from urllib.parse import unquote

import aiofiles
import fastapi
import fastapi.responses
from fastapi.responses import PlainTextResponse
from fastapi import APIRouter, Path, Query, Request, Depends, File, Form, HTTPException, UploadFile, Body
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.FileLock import FileLock


logger = logging.getLogger(__name__)
router = APIRouter(tags=["upload"])
#router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["upload"])


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


# upload complete file
@router.post("/upload")
async def upload(
    # upload file parameters
    uploadFile: UploadFile = File(...),
    uploadId: str = Form(None),
    hashType: HashType = Form(None),
    hashDigest: str = Form(None),
    copyMode: str = Form("native"),
    # save file parameters
    depId: str = Form(...),
    repositoryType: str = Form(...),
    contentType: str = Form(...),
    milestone: str = Form(None),
    partNumber: int = Form(...),
    contentFormat: str = Form(...),
    version: str = Form(None),
    allowOverwrite: bool = Form(None)
):
    tempPath = None
    outPath = None
    ifh = uploadFile.file
    ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
    try:
        fn = uploadFile.filename
        ct = uploadFile.content_type
        # if fn.endswith(".gz") or ct == "application/gzip":
        #     copyMode = "gzip_decompress"
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        if not pathU.checkContentTypeFormat(contentType, contentFormat):
            raise HTTPException(status_code=405, detail = "Bad content type and/or format - upload rejected")
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
            raise HTTPException(status_code=405, detail = "Bad content type metadata - cannot build a valid path")
        if os.path.exists(outPath) and not allowOverwrite:
            raise HTTPException(status_code=405, detail = "Encountered existing file - overwrite prohibited")
        dirPath, fn = os.path.split(outPath)
        tempPath = os.path.join(dirPath, "." + fn)
        os.makedirs(dirPath, mode=0o755, exist_ok=True)
        if copyMode == "shell" or copyMode == "native" or copyMode == "gzip_decompress":
            with open(tempPath, "wb") as ofh:
                shutil.copyfileobj(ifh, ofh)
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
            hexdigest = hashObj.hexdigest()
            ok = (hexdigest == hashDigest)
            if not ok:
                raise HTTPException(status_code=400, detail = f"{hashType} hash check failed {hexdigest} != {hashDigest}")
            else:
                os.replace(tempPath, outPath)
                if copyMode == "gzip_decompress":
                    with gzip.open(outPath, "rb") as r:
                        with open(tempPath, "wb") as w:
                            w.write(r.read())
                    os.replace(tempPath, outPath)
            if os.path.exists(tempPath):
                os.unlink(tempPath)
        else:
            raise HTTPException(status_code=500, detail = "Error - missing hash")
    except HTTPException as e:
        ret = {"success": False, "statusCode": e.status_code, "statusMessage": f"Store fails with {e.detail}"}
        logging.warning(ret["statusMessage"])
    except Exception as e:
        ret = {"success": False, "statusCode": 400, "statusMessage": f"Store fails with {str(e)}"}
        logging.warning(ret["statusMessage"])
    finally:
        ifh.close()
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    return ret


@router.get("/getNewUploadId")
async def getNewUploadId():
    return {"id": uuid.uuid4().hex}
    #return PlainTextResponse(str(uuid.uuid4().hex))


@router.get("/getSaveFilePath")
async def getSaveFilePath(repositoryType: str = Query(...),
                          depId: str = Query(...),
                          contentType: str = Query(...),
                          milestone: Optional[str] = Query(default="next"),
                          partNumber: int = Query(...),
                          contentFormat: str = Query(...),
                          version: str = Query(default="next"),
                          allowOverwrite: bool = Query(default=False)):
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    pathU = PathUtils(cP)
    if not pathU.checkContentTypeFormat(contentType, contentFormat):
        raise HTTPException(status_code=400, detail="Bad content type and/or format - upload rejected")
    lockPath = pathU.getFileLockPath(depId, contentType, milestone, partNumber, contentFormat)
    outPath = None
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
        raise HTTPException(status_code=400, detail="Bad content type metadata - cannot build a valid path")
    if os.path.exists(outPath) and not allowOverwrite:
        raise HTTPException(status_code=400, detail="Encountered existing file - overwrite prohibited")
    dirPath, fn = os.path.split(outPath)
    os.makedirs(dirPath, mode=0o755, exist_ok=True)
    return {"path": outPath}
    #return PlainTextResponse(outPath)

# upload chunk
@router.post("/sequentialUpload")
async def sequentialUpload(
    # upload file parameters
    uploadFile: UploadFile = File(...),
    uploadId: str = Form(None),
    hashType: str = Form(None),
    hashDigest: str = Form(None),
    copyMode: str = Form("native"),
    # chunk parameters
    chunkSize: int = Form(None),
    chunkIndex: int = Form(None),
    chunkOffset: int = Form(None),
    expectedChunks: int = Form(None),
    chunkMode: str = Form("sequential"),
    # save file parameters
    filePath: str = Form(...)
):
    contents = await uploadFile.read()
    if len(contents) <= 0:
        raise HTTPException(status_code=400, detail="error - empty file")
    ret = {"success": True, "statusCode": 200, "statusMessage": "Chunk uploaded"}
    fn = uploadFile.filename
    ct = uploadFile.content_type
    # if fn.endswith(".gz") or ct == "application/gzip":
    #    copyMode = "gzip_decompress"
    dirPath, fn = os.path.split(filePath)
    tempPath = os.path.join(dirPath, "." + uploadId)
    async with aiofiles.open(tempPath, "ab") as ofh:
        await ofh.seek(chunkOffset)
        await ofh.write(contents)
        await ofh.flush()
        os.fsync(ofh.fileno())
    if chunkIndex + 1 == expectedChunks:
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
            hexdigest = hashObj.hexdigest()
            ok = (hexdigest == hashDigest)
            if not ok:
                raise HTTPException(status_code=400, detail=f"{hashType} hash check failed")
            else:
                os.replace(tempPath, filePath)
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            if copyMode == "gzip_decompress":
                with gzip.open(filePath, "rb") as r:
                    with open(tempPath, "wb") as w:
                        w.write(r.read())
                os.replace(tempPath, filePath)
        else:
            raise HTTPException(status_code=500, detail="Error - missing hash")
    return ret


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


@router.post("/asyncUpload", response_model=UploadResult)
async def asyncUpload(
    # upload file parameters
    uploadFile: UploadFile = File(...),
    uploadId: str = Form(None),
    hashType: HashType = Form(None),
    hashDigest: str = Form(None),
    copyMode: str = Form("native"),
    # chunk parameters
    chunkSize: int = Form(None),
    chunkIndex: int = Form(0),
    chunkOffset: int = Form(0),
    expectedChunks: int = Form(1),
    chunkMode: str = Form("sequential"),
    # save file parameters
    depId: str = Form(...),
    repositoryType: str = Form(...),
    contentType: str = Form(...),
    milestone: str = Form(None),
    partNumber: int = Form(...),
    contentFormat: str = Form(...),
    version: str = Form(...),
    allowOverwrite: bool = Form(None),
    emailAddress: str = Form(None)
):
    fn = None
    ct = None
    try:
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        logger.debug("depId %r hash %r hashType %r", depId, hashDigest, hashType)
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("uploadFile %s (%r)", fn, ct)
        # if fn.endswith(".gz") or ct == "application/gzip":
        #     copyMode = "gzip_decompress"
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
            allowOverwrite=allowOverwrite,
            emailAddress=emailAddress
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {
            "success": False,
            "statusCode": 400,
            "statusMessage": f"Upload fails with {str(e)}",
        }
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    return ret
