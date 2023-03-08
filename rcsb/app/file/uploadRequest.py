##
# File: uploadRequest.py
# Date: 27-Oct-2022
#
##
__docformat__ = "google en"
__author__ = "James Smith, Ahsan Tanweer"
__email__ = "james.smith@rcsb.org, ahsan@ebi.ac.uk"
__license__ = "Apache 2.0"

import gzip
import hashlib
import logging
import os
import re
import uuid
import json
from enum import Enum
from typing import Optional
from filelock import Timeout, FileLock
import aiofiles
from fastapi import APIRouter, Query, File, Form, HTTPException, UploadFile, Depends
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
import rcsb.app.config.setConfig  # noqa: F401 pylint: disable=W0611
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.PathUtils import PathUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer

logger = logging.getLogger(__name__)

# CONFIG_FILE = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "config.yml"))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", CONFIG_FILE)
provider = ConfigProvider(os.environ.get("CONFIG_FILE"))
jwtDisable = bool(provider.get('JWT_DISABLE'))
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
    resumable: bool = Form(False),
    # save file parameters
    filePath: str = Form(...),
    decompress: bool = Form(False),
    allowOverwrite: bool = Form(False),
):
    if resumable:
        # IoUtils has database functions
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        ioU = IoUtils(cP)
        ret = await ioU.upload(
            # chunk parameters
            chunk=chunk.file,
            chunkSize=chunkSize,
            chunkIndex=chunkIndex,
            expectedChunks=expectedChunks,
            # upload file parameters
            uploadId=uploadId,
            hashType=hashType,
            hashDigest=hashDigest,
            resumable=resumable,
            # save file parameters
            filePath=filePath,
            decompress=decompress,
            allowOverwrite=allowOverwrite,
        )
        return ret
    # expedite upload rather than instantiating ConfigProvider, IoUtils for each chunk, then passing file parameter
    chunkOffset = chunkIndex * chunkSize
    ret = {"success": True, "statusCode": 200, "statusMessage": "Chunk uploaded"}
    dirPath, _ = os.path.split(filePath)
    tempPath = getTempFilePath(uploadId, dirPath)
    contents = await chunk.read()
    # empty chunk beyond loop index from client side, don't erase tempPath so keep out of try block
    if contents and len(contents) <= 0:
        raise HTTPException(status_code=400, detail="error - empty file")
    try:
        # save, then hash, then decompress
        # should lock, however client must wait for each response before sending next chunk, precluding race conditions (unless multifile upload problem)
        async with aiofiles.open(tempPath, "ab") as ofh:
            await ofh.seek(chunkOffset)
            await ofh.write(contents)
            await ofh.flush()
            os.fsync(ofh.fileno())
        # if last chunk
        if chunkIndex + 1 == expectedChunks:
            if hashDigest and hashType:
                if hashType == "SHA1":
                    hashObj = hashlib.sha1()
                elif hashType == "SHA256":
                    hashObj = hashlib.sha256()
                elif hashType == "MD5":
                    hashObj = hashlib.md5()
                blockSize = 65536
                # hash temp file
                with open(tempPath, "rb") as r:
                    while hash_chunk := r.read(blockSize):
                        hashObj.update(hash_chunk)
                hexdigest = hashObj.hexdigest()
                ok = (hexdigest == hashDigest)
                if not ok:
                    raise HTTPException(status_code=400, detail=f"{hashType} hash check failed")
                else:
                    # lock then save
                    lockPath = os.path.join(os.path.dirname(filePath), "." + os.path.basename(filePath) + ".lock")
                    lock = FileLock(lockPath)
                    try:
                        with lock.acquire(timeout=60 * 60 * 4):
                            # last minute race condition handling
                            if os.path.exists(filePath) and not allowOverwrite:
                                raise HTTPException(status_code=400, detail="Encountered existing file - cannot overwrite")
                            else:
                                # save final version
                                os.replace(tempPath, filePath)
                    except Timeout:
                        raise HTTPException(status_code=400, detail=f"error - lock timed out on {filePath}")
                    finally:
                        lock.release()
                        if os.path.exists(lockPath):
                            os.unlink(lockPath)
                # remove temp file
                if os.path.exists(tempPath):
                    os.unlink(tempPath)
                # decompress
                if decompress:
                    with gzip.open(filePath, "rb") as r:
                        with open(tempPath, "wb") as w:
                            w.write(r.read())
                    os.replace(tempPath, filePath)
            else:
                raise HTTPException(status_code=500, detail="Error - missing hash")
    except HTTPException as exc:
        if os.path.exists(tempPath):
            os.unlink(tempPath)
        ret = {"success": False, "statusCode": exc.status_code, "statusMessage": f"error in sequential upload {exc.detail}"}
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except Exception as exc:
        if os.path.exists(tempPath):
            os.unlink(tempPath)
        ret = {"success": False, "statusCode": 400, "statusMessage": f"error in sequential upload {str(exc)}"}
        raise HTTPException(status_code=400, detail=f"error in sequential upload {str(exc)}")
    return ret


# should match same function in IoUtils.py
def getTempFilePath(uploadId, dirPath):
    return os.path.join(dirPath, "._" + uploadId)


@router.get("/getUploadParameters")
async def getUploadParameters(
        repositoryType: str = Query(...),
        depId: str = Query(...),
        contentType: str = Query(...),
        milestone: Optional[str] = Query(default="next"),
        partNumber: int = Query(...),
        contentFormat: str = Query(...),
        version: str = Query(default="next"),
        hashDigest: str = Query(default=None),
        allowOverwrite: bool = Query(default=True),
        resumable: bool = Query(default=False)
):
    chunkIndex, uploadId = await getUploadStatus(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, hashDigest, resumable)
    filePath = await getSaveFilePath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite)
    if not filePath:
        raise HTTPException(status_code=400, detail="Error - could not make file path from parameters")
    return {"filePath": filePath, "chunkIndex": chunkIndex, "uploadId": uploadId}


# return kv entry from file parameters, if have resumed upload, or None if don't
# if have resumed upload, kv response has chunk count
async def getUploadStatus(repositoryType: str,
                          depId: str,
                          contentType: str,
                          milestone: str,
                          partNumber: int,
                          contentFormat: str,
                          version: str,
                          hashDigest: str,
                          resumable: bool
                          ):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
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
    uploadCount = 0
    uploadId = None
    if resumable:
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
    if uploadId:
        status = await ioU.getSession(uploadId)
        if status:
            status = str(status)
            status = status.replace("'", '"')
            status = json.loads(status)
            uploadCount = status["uploadCount"]
    else:
        uploadId = getNewUploadId()
    return int(uploadCount), uploadId


async def getSaveFilePath(repositoryType: str,
                          depId: str,
                          contentType: str,
                          milestone: str,
                          partNumber: int,
                          contentFormat: str,
                          version: str,
                          allowOverwrite: bool):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    pathU = PathUtils(cP)
    if not pathU.checkContentTypeFormat(contentType, contentFormat):
        logging.warning("Bad content type and/or format - upload rejected")
        return None
    outPath = None
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
        logging.warning("Bad content type metadata - cannot build a valid path")
        return None
    if os.path.exists(outPath) and not allowOverwrite:
        logging.warning("Encountered existing file - overwrite prohibited")
        return None
    dirPath, _ = os.path.split(outPath)
    os.makedirs(dirPath, mode=0o777, exist_ok=True)
    return outPath


def getNewUploadId():
    return uuid.uuid4().hex


# clear kv entries from one user
@router.post("/clearSession")
async def clearSession(uploadIds: list = Form(...)):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    ioU = IoUtils(cP)
    return await ioU.clearSession(uploadIds, None)


# purge kv before testing
@router.post("/clearKv")
async def clearKv():
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    ioU = IoUtils(cP)
    return await ioU.clearKv()
