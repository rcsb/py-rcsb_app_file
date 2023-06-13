##
# File:    UploadUtility.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates: James Smith, Ahsan Tanweer 2023
#

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import datetime
import gzip
import logging
import os
import typing
import uuid
import aiofiles
import json
from filelock import Timeout, FileLock
from fastapi import HTTPException
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.IoUtility import IoUtility
from rcsb.app.file.KvSqlite import KvSqlite
from rcsb.app.file.KvRedis import KvRedis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()

# functions -
# get upload parameters, upload, check content type format, helper resumable upload functions, helper database functions


class UploadUtility(object):
    def __init__(self, cP: typing.Type[ConfigProvider] = None):
        self.cP = cP if cP else ConfigProvider()

    async def getUploadParameters(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: typing.Optional[str],
        partNumber: int,
        contentFormat: str,
        version: str,
        allowOverwrite: bool,
        resumable: bool,
    ):
        # get save file path
        pP = PathProvider(self.cP)
        if not pP.checkContentTypeFormat(contentType, contentFormat):
            logging.error("Error 400 - bad content type and/or format")
            raise HTTPException(
                status_code=400, detail="Error - bad content type and/or format"
            )
        outPath = pP.getVersionedPath(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
        )
        if not outPath:
            logging.error("Error 400 - could not make file path from parameters")
            raise HTTPException(
                status_code=400,
                detail="Error - could not make file path from parameters",
            )
        if os.path.exists(outPath) and not allowOverwrite:
            logger.exception(
                "Error 403 - encountered existing file - overwrite prohibited"
            )
            raise HTTPException(
                status_code=403,
                detail="Encountered existing file - overwrite prohibited",
            )
        dirPath, _ = os.path.split(outPath)
        defaultFilePermissions = self.cP.get("DEFAULT_FILE_PERMISSIONS")
        repositoryPath = self.cP.get("REPOSITORY_DIR_PATH")
        os.makedirs(dirPath, mode=defaultFilePermissions, exist_ok=True)
        if outPath.startswith(repositoryPath):
            outPath = outPath.replace(repositoryPath, "")
            outPath = outPath[1:]
        else:
            raise HTTPException(
                status_code=400, detail="Error in file path formation %s" % outPath
            )
        # get upload id
        uploadId = None
        if resumable:
            uploadId = await self.getResumedUpload(
                repositoryType=repositoryType,
                depId=depId,
                contentType=contentType,
                milestone=milestone,
                partNumber=partNumber,
                contentFormat=contentFormat,
                version=version,
                pP=pP,
            )
        if not uploadId:
            uploadId = self.getNewUploadId()
        # get chunk index
        uploadCount = 0
        if uploadId:
            status = await self.getSession(uploadId)
            if status:
                status = str(status)
                status = status.replace("'", '"')
                status = json.loads(status)
                if "chunkSize" in status:
                    chunkSize = int(status["chunkSize"])
                    tempPath = self.getTempFilePath(uploadId, dirPath)
                    if os.path.exists(tempPath):
                        fileSize = os.path.getsize(tempPath)
                        uploadCount = round(fileSize / chunkSize)
        return {"filePath": outPath, "chunkIndex": uploadCount, "uploadId": uploadId}

    # in-place sequential chunk
    async def upload(
        self,
        # chunk parameters
        chunk: typing.IO,
        chunkSize: int,
        chunkIndex: int,
        expectedChunks: int,
        # upload file parameters
        uploadId: str,
        hashType: str,
        hashDigest: str,
        # save file parameters
        filePath: str,
        decompress: bool,
        allowOverwrite: bool,
        # other
        resumable: bool,
    ):
        repositoryPath = self.cP.get("REPOSITORY_DIR_PATH")
        filePath = os.path.join(repositoryPath, filePath)
        if resumable:
            repositoryType = os.path.basename(
                os.path.dirname(os.path.dirname(filePath))
            )
            key = uploadId
            logKey = self.getPremadeLogKey(repositoryType, filePath)
            # on first chunk upload, set chunk size, record uid in log table
            if chunkIndex == 0:
                if self.cP.get("KV_MODE") == "sqlite":
                    kV = KvSqlite(self.cP)
                elif self.cP.get("KV_MODE") == "redis":
                    kV = KvRedis(self.cP)
                kV.setSession(key, "chunkSize", chunkSize)
                kV.setLog(logKey, uploadId)
                kV.setSession(
                    key,
                    "timestamp",
                    int(
                        datetime.datetime.timestamp(
                            datetime.datetime.now(datetime.timezone.utc)
                        )
                    ),
                )

        logger.debug("chunk %s of %s for %s", chunkIndex, expectedChunks, uploadId)

        dirPath, _ = os.path.split(filePath)
        tempPath = self.getTempFilePath(uploadId, dirPath)
        contents = chunk.read()
        # empty chunk beyond loop index from client side, don't erase temp file so keep out of try block
        if contents and len(contents) <= 0:
            # outside of try block an exception will exit
            chunk.close()
            raise HTTPException(status_code=400, detail="error - empty file")
        try:
            # save, then hash, then decompress
            # should lock, however client must wait for each response before sending next chunk, precluding race conditions (unless multifile upload problem)
            async with aiofiles.open(tempPath, "ab") as ofh:
                await ofh.write(contents)
            # if last chunk
            if chunkIndex + 1 == expectedChunks:
                if not hashDigest and hashType:
                    raise HTTPException(status_code=400, detail="Error - missing hash")
                if not IoUtility().checkHash(tempPath, hashDigest, hashType):
                    raise HTTPException(
                        status_code=400, detail=f"{hashType} hash check failed"
                    )
                # lock then save
                lockPath = os.path.join(
                    os.path.dirname(filePath),
                    "." + os.path.basename(filePath) + ".lock",
                )
                lock = FileLock(lockPath)
                try:
                    with lock.acquire(timeout=60 * 60 * 4):
                        # last minute race condition handling
                        if os.path.exists(filePath) and not allowOverwrite:
                            raise HTTPException(
                                status_code=403,
                                detail="Encountered existing file - cannot overwrite",
                            )
                        else:
                            # save final version
                            os.replace(tempPath, filePath)
                except Timeout:
                    raise HTTPException(
                        status_code=400,
                        detail=f"error - lock timed out on {filePath}",
                    )
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
                # clear database
                if resumable:
                    self.clearSession(key, logKey)
        except HTTPException as exc:
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            if resumable:
                self.clearSession(key, logKey)
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        except Exception as exc:
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            if resumable:
                self.clearSession(key, logKey)
            raise HTTPException(
                status_code=400, detail=f"error in sequential upload {str(exc)}"
            )
        finally:
            chunk.close()

    def getNewUploadId(self):
        return uuid.uuid4().hex

    # file path functions

    def getTempFilePath(self, uploadId, dirPath):
        return os.path.join(dirPath, "._" + uploadId)

    def getPrimaryLogKey(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
        version: str = "next",
        pP=None,
    ):
        if not pP:
            pP = PathProvider(self.cP)
        filename = pP.getVersionedPath(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
        )
        if not filename:
            return None
        filename = os.path.basename(filename)
        filename = repositoryType + "_" + filename
        return filename

    def getPremadeLogKey(self, repositoryType, versionedPath):
        # when versioned path is already found
        if not versionedPath:
            return None
        filename = os.path.basename(versionedPath)
        filename = repositoryType + "_" + filename
        return filename

    # database functions

    # find upload id using file parameters
    async def getResumedUpload(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
        version: str = "next",
        pP=None,
        kV=None,
    ):
        filename = self.getPrimaryLogKey(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
            pP=pP,
        )
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        uploadId = kV.getLog(filename)
        if not uploadId:
            # not a resumed upload
            return None
        # remove expired entries
        timestamp = int(kV.getSession(uploadId, "timestamp"))
        now = datetime.datetime.timestamp(datetime.datetime.now(datetime.timezone.utc))
        duration = now - timestamp
        max_duration = self.cP.get("KV_MAX_SECONDS")
        if duration > max_duration:
            await self.removeExpiredEntry(
                uploadId=uploadId,
                fileName=filename,
                depId=depId,
                repositoryType=repositoryType,
                kV=kV,
                pP=pP,
            )
            return None
        # returns uploadId or None
        return uploadId

    # remove an entry from session table and log table, remove corresponding hidden files
    # does not check expiration
    async def removeExpiredEntry(
        self,
        uploadId: str = None,
        fileName: str = None,
        depId: str = None,
        repositoryType: str = None,
        kV=None,
        pP=None,
    ):
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        # remove expired entry and temp files
        kV.clearSessionKey(uploadId)
        # still must remove log table entry (key = file parameters)
        if self.cP.get("KV_MODE") == "sqlite":
            kV.clearLogVal(uploadId)
        elif self.cP.get("KV_MODE") == "redis":
            kV.clearLog(fileName)
        if not pP:
            pP = PathProvider(self.cP)
        dirPath = pP.getDirPath(repositoryType, depId)
        try:
            tempFile = self.getTempFilePath(uploadId, dirPath)
            os.unlink(tempFile)
        except Exception:
            # tempFile was not found
            pass

    # returns entire dictionary of session table entry
    async def getSession(self, uploadId: str, kV=None):
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        return kV.getKey(uploadId, kV.sessionTable)

    # clear one entry from session table
    async def clearUploadId(self, uid: str, kV=None):
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        response = None
        try:
            response = kV.clearSessionKey(uid)
        except Exception:
            return False
        return response

    # clear one entry from session table and corresponding entry from log table
    def clearSession(self, uid: str, logKey: str, kV=None):
        response = True
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        try:
            res = kV.clearSessionKey(uid)
            if not res:
                response = False
            if self.cP.get("KV_MODE") == "sqlite":
                kV.clearLogVal(uid)
            elif self.cP.get("KV_MODE") == "redis":
                kV.clearLog(logKey)
        except Exception:
            return False
        return response

    # clear entire database
    async def clearKv(self, kV=None):
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        kV.clearTable(kV.sessionTable)
        kV.clearTable(kV.logTable)
