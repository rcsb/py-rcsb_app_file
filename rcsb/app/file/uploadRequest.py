##
# File: uploadRequest.py
# Date: 27-Oct-2022
#
##
__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import gzip
import hashlib
import logging
import os
import re
import shutil
import uuid
import json
from enum import Enum
from typing import Optional
from filelock import Timeout, FileLock
import aiofiles
from fastapi import APIRouter, Path, Query, File, Form, HTTPException, UploadFile, Depends
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.PathUtils import PathUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer


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


# upload complete file
@router.post("/upload")
async def upload(
    # upload file parameters
    uploadFile: UploadFile = File(...),
    # uploadId: str = Form(None),
    hashType: HashType = Form(None),
    hashDigest: str = Form(None),
    # save file parameters
    depId: str = Form(...),
    repositoryType: str = Form(...),
    contentType: str = Form(...),
    milestone: str = Form(None),
    partNumber: int = Form(...),
    contentFormat: str = Form(...),
    version: str = Form(None),
    copyMode: str = Form("native"),
    allowOverwrite: bool = Form(None)
):
    tempPath = None
    outPath = None
    ifh = uploadFile.file
    ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
    try:
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
        if not pathU.checkContentTypeFormat(contentType, contentFormat):
            raise HTTPException(status_code=405, detail="Bad content type and/or format - upload rejected")
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
            raise HTTPException(status_code=405, detail="Bad content type metadata - cannot build a valid path")
        if os.path.exists(outPath) and not allowOverwrite:
            raise HTTPException(status_code=405, detail="Encountered existing file - overwrite prohibited")
        dirPath, _ = os.path.split(outPath)
        uploadId = await getNewUploadId()
        uploadId = uploadId["id"]
        tempPath = os.path.join(dirPath, "." + uploadId)
        os.makedirs(dirPath, mode=0o777, exist_ok=True)
        # save (all copy modes), then hash, then decompress
        with open(tempPath, "wb") as ofh:
            shutil.copyfileobj(ifh, ofh)
        # hash
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
                raise HTTPException(status_code=400, detail=f"{hashType} hash check failed {hexdigest} != {hashDigest}")
            else:
                # lock before saving
                lockPath = os.path.join(os.path.dirname(outPath), "." + os.path.basename(outPath) + ".lock")
                lock = FileLock(lockPath)
                try:
                    with lock.acquire(timeout=60 * 60 * 4):
                        # last minute race condition prevention
                        if os.path.exists(outPath) and not allowOverwrite:
                            raise HTTPException(status_code=400, detail="Encountered existing file - cannot overwrite")
                        else:
                            # save final version
                            os.replace(tempPath, outPath)
                except Timeout:
                    raise HTTPException(status_code=400, detail=f"error - file lock timed out on {outPath}")
                finally:
                    lock.release()
                    if os.path.exists(lockPath):
                        os.unlink(lockPath)
                # decompress
                if copyMode == "gzip_decompress":
                    with gzip.open(outPath, "rb") as r:
                        with open(tempPath, "wb") as w:
                            w.write(r.read())
                    os.replace(tempPath, outPath)
        else:
            raise HTTPException(status_code=500, detail="Error - missing hash")
    except HTTPException as exc:
        ret = {"success": False, "statusCode": exc.status_code, "statusMessage": f"Store fails with {exc.detail}"}
        logging.warning(ret["statusMessage"])
    except Exception as exc:
        ret = {"success": False, "statusCode": 400, "statusMessage": f"Store fails with {str(exc)}"}
        logging.warning(ret["statusMessage"])
    finally:
        if tempPath and os.path.exists(tempPath):
            os.unlink(tempPath)
        ifh.close()
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    return ret


@router.get("/getNewUploadId")
async def getNewUploadId():
    return {"id": uuid.uuid4().hex}


@router.get("/getSaveFilePath")
async def getSaveFilePath(repositoryType: str = Query(...),
                          depId: str = Query(...),
                          contentType: str = Query(...),
                          milestone: Optional[str] = Query(default="next"),
                          partNumber: int = Query(...),
                          contentFormat: str = Query(...),
                          version: str = Query(default="next"),
                          allowOverwrite: bool = Query(default=False)):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    pathU = PathUtils(cP)
    if not pathU.checkContentTypeFormat(contentType, contentFormat):
        raise HTTPException(status_code=400, detail="Bad content type and/or format - upload rejected")
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
        raise HTTPException(status_code=400, detail="Bad content type metadata - cannot build a valid path")
    if os.path.exists(outPath) and not allowOverwrite:
        raise HTTPException(status_code=400, detail="Encountered existing file - overwrite prohibited")
    dirPath, _ = os.path.split(outPath)
    os.makedirs(dirPath, mode=0o777, exist_ok=True)
    return {"path": outPath}


# upload chunk
@router.post("/sequentialUpload")
async def sequentialUpload(
    # upload file parameters
    uploadFile: UploadFile = File(...),
    uploadId: str = Form(None),
    hashType: str = Form(None),
    hashDigest: str = Form(None),
    # chunk parameters
    chunkSize: int = Form(None),
    chunkIndex: int = Form(None),
    expectedChunks: int = Form(None),
    # save file parameters
    filePath: str = Form(...),
    copyMode: str = Form("native"),
    allowOverwrite: bool = Query(default=False)
):
    chunkOffset = chunkIndex * chunkSize
    ret = {"success": True, "statusCode": 200, "statusMessage": "Chunk uploaded"}
    dirPath, _ = os.path.split(filePath)
    tempPath = os.path.join(dirPath, "." + uploadId)
    contents = await uploadFile.read()
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
                # hash
                with open(tempPath, "rb") as r:
                    while chunk := r.read(blockSize):
                        hashObj.update(chunk)
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
                        raise HTTPException(status_code=400, detail=f'error - lock timed out on {filePath}')
                    finally:
                        lock.release()
                        if os.path.exists(lockPath):
                            os.unlink(lockPath)
                # remove temp file
                if os.path.exists(tempPath):
                    os.unlink(tempPath)
                # decompress
                if copyMode == "gzip_decompress":
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
        raise HTTPException(status_code=400, detail=f'error in sequential upload {str(exc)}')
    return ret


# return kv entry from file parameters, if have resumed upload, or None if don't
# if have resumed upload, kv response has chunk indices and count
@router.get("/uploadStatus")
async def getUploadStatus(repositoryType: str = Query(...),
                          depId: str = Query(...),
                          contentType: str = Query(...),
                          milestone: Optional[str] = Query(default="next"),
                          partNumber: int = Query(...),
                          contentFormat: str = Query(...),
                          version: str = Query(default="next"),
                          hashDigest: str = Query(default=None)
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
    status = status.replace("'", '"')
    status = json.loads(status)
    return status


# return kv entry from upload id
@router.get("/uploadStatusFromId/{uploadId}")
async def getUploadStatusFromId(uploadId: str = Path(...)):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    ioU = IoUtils(cP)
    status = await ioU.getSession(uploadId)
    return status


# find upload id from file parameters
@router.get("/findUploadId")
async def findUploadId(repositoryType: str = Query(),
                       depId: str = Query(...),
                       contentType: str = Query(...),
                       milestone: str = Query(...),
                       partNumber: int = Query(...),
                       contentFormat: str = Query(...),
                       version: str = Query(...)):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    ioU = IoUtils(cP)
    return ioU.getResumedUpload(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)


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


@router.post("/resumableUpload")  # response_model=UploadResult)
async def resumableUpload(
    # upload file parameters
    uploadFile: UploadFile = File(...),
    uploadId: str = Form(None),
    hashType: HashType = Form(None),
    hashDigest: str = Form(None),
    # chunk parameters
    chunkSize: int = Form(None),
    chunkIndex: int = Form(0),
    expectedChunks: int = Form(1),
    # save file parameters
    depId: str = Form(...),
    repositoryType: str = Form(...),
    contentType: str = Form(...),
    milestone: str = Form(None),
    partNumber: int = Form(...),
    contentFormat: str = Form(...),
    version: str = Form(...),
    copyMode: str = Form("native"),
    allowOverwrite: bool = Form(None),
):
    fn = None
    ct = None
    try:
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        logger.debug("depId %r hash %r hashType %r", depId, hashDigest, hashType)
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("uploadFile %s (%r)", fn, ct)
        logger.debug("hashType.name %r hashDigest %r", hashType, hashDigest)
        ioU = IoUtils(cP)
        ret = await ioU.resumableUpload(
            # upload file parameters
            ifh=uploadFile.file,
            uploadId=uploadId,
            hashType=hashType,
            hashDigest=hashDigest,
            copyMode=copyMode,
            # chunk parameters
            chunkSize=chunkSize,
            chunkIndex=chunkIndex,
            expectedChunks=expectedChunks,
            # save file parameters
            depId=depId,
            repositoryType=repositoryType,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
            allowOverwrite=allowOverwrite,
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
