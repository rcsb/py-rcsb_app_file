##
# File:    IoUtils.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates: James Smith, Ahsan Tanweer 2023
#
"""
Collected I/O utilities.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import datetime
import gzip
import hashlib
import logging
import os
import typing
import uuid
import aiofiles
import re
import json
from filelock import Timeout, FileLock
from fastapi import HTTPException
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathUtils import PathUtils
from rcsb.app.file.KvSqlite import KvSqlite
from rcsb.app.file.KvRedis import KvRedis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class IoUtils:
    """Collected utilities request I/O processing.

    1. Upload a single file (only write new versions)
        lock the depId/contentType
            create the output path
            store data atomically

    2. Upload a single file in parts (only write new versions):
        if a client multipart partSession exists use it otherwise create the new client session
        save part and metadata to new session

        if all parts are present
            assemble parts and do the single file update (above)

    3. Upload tarfile of individual files with common type/format w/ version=next

    Download a single file
    Download/upload a session bundle
    """

    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.__cP = cP
        if self.__cP.get("KV_MODE") == "sqlite":
            self.__kV = KvSqlite(self.__cP)
        elif self.__cP.get("KV_MODE") == "redis":
            self.__kV = KvRedis(self.__cP)
        self.__pathU = PathUtils(self.__cP)

    def checkHash(self, pth: str, hashDigest: str, hashType: str) -> bool:
        tHash = self.getHashDigest(pth, hashType)
        return tHash == hashDigest

    def getHashDigest(self, filePath: str, hashType: str = "SHA1", blockSize: int = 65536) -> typing.Optional[str]:
        """Return the hash (hashType) for the input file.

        Args:
            filePath (str): for input file path
            hashType (str, optional): one of MD5, SHA1, or SHA256. Defaults to "SHA1".
            blockSize (int, optional): the size of incremental read operations. Defaults to 65536.

        Returns:
            (str): hash digest or None on failure
        """
        if hashType not in ["MD5", "SHA1", "SHA256"]:
            return None
        try:
            if hashType == "SHA1":
                hashObj = hashlib.sha1()
            elif hashType == "SHA256":
                hashObj = hashlib.sha256()
            elif hashType == "MD5":
                hashObj = hashlib.md5()

            with open(filePath, "rb") as ifh:
                for block in iter(lambda: ifh.read(blockSize), b""):
                    hashObj.update(block)
            return hashObj.hexdigest()
        except Exception as e:
            logger.exception("Failing with file %s %r", filePath, str(e))
        return None

    # in-place sequential chunk
    async def upload(self,
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
            allowOverwrite: bool
    ):
        # chunkOffset = chunkIndex * chunkSize
        # remove comment for testing
        # logger.info(f"chunk {chunkIndex} of {expectedChunks} for {uploadId}")
        ret = {"success": True, "statusCode": 200, "statusMessage": "Chunk uploaded"}
        dirPath, _ = os.path.split(filePath)
        tempPath = self.getTempFilePath(uploadId, dirPath)
        contents = await chunk.read()
        # empty chunk beyond loop index from client side, don't erase tempPath so keep out of try block
        if contents and len(contents) <= 0:
            raise HTTPException(status_code=400, detail="error - empty file")
        try:
            # save, then hash, then decompress
            # should lock, however client must wait for each response before sending next chunk, precluding race conditions (unless multifile upload problem)
            async with aiofiles.open(tempPath, "ab") as ofh:
                await ofh.write(contents)
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
                                    raise HTTPException(status_code=400,
                                                        detail="Encountered existing file - cannot overwrite")
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
            ret = {"success": False, "statusCode": exc.status_code,
                   "statusMessage": f"error in sequential upload {exc.detail}"}
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        except Exception as exc:
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            ret = {"success": False, "statusCode": 400, "statusMessage": f"error in sequential upload {str(exc)}"}
            raise HTTPException(status_code=400, detail=f"error in sequential upload {str(exc)}")
        finally:
            chunk.close()
        return ret


    def getTempFilePath(self, uploadId, dirPath):
        return os.path.join(dirPath, "._" + uploadId)

    # file path functions

    async def findVersion(self,
                          repositoryType: str = "archive",
                          depId: str = None,
                          contentType: str = "model",
                          milestone: str = None,
                          partNumber: int = 1,
                          contentFormat: str = "pdbx",
                          version: str = "next"
                          ):
        primaryKey = self.getPrimaryLogKey(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version
        )
        versions = primaryKey.split(".")
        version = versions[-1]
        version = version.replace("V", "")
        return version

    def getPrimaryLogKey(self,
                         repositoryType: str = "archive",
                         depId: str = None,
                         contentType: str = "model",
                         milestone: str = None,
                         partNumber: int = 1,
                         contentFormat: str = "pdbx",
                         version: str = "next"
                         ):
        filename = self.__pathU.getVersionedPath(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version
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
    async def getResumedUpload(self,
                               repositoryType: str = "archive",
                               depId: str = None,
                               contentType: str = "model",
                               milestone: str = None,
                               partNumber: int = 1,
                               contentFormat: str = "pdbx",
                               version: str = "next",
                               hashDigest: str = None
                               ):
        filename = self.getPrimaryLogKey(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version
        )
        uploadId = self.__kV.getLog(filename)
        if not uploadId:
            return None
        # remove expired entries
        timestamp = int(self.__kV.getSession(uploadId, "timestamp"))
        now = datetime.datetime.timestamp(datetime.datetime.now(datetime.timezone.utc))
        duration = now - timestamp
        max_duration = self.__cP.get("KV_MAX_SECONDS")
        if duration > max_duration:
            await self.removeExpiredEntry(uploadId=uploadId, fileName=filename, depId=depId, repositoryType=repositoryType)
            return None
        # test if user resumes with same file as previously
        if hashDigest is not None:
            hashvar = self.__kV.getSession(uploadId, "hashDigest")
            if hashvar != hashDigest:
                await self.removeExpiredEntry(uploadId=uploadId, fileName=filename, depId=depId, repositoryType=repositoryType)
                return None
        else:
            logging.warning("error - no hash")
        return uploadId  # returns uploadId or None

    # remove an entry from session table and log table, remove corresponding hidden files
    # does not check expiration
    async def removeExpiredEntry(self,
                                 uploadId: str = None,
                                 fileName: str = None,
                                 depId: str = None,
                                 repositoryType: str = None
                                 ):
        # remove expired entry and temp files
        self.__kV.clearSessionKey(uploadId)
        # still must remove log table entry (key = file parameters)
        if self.__cP.get("KV_MODE") == "sqlite":
            self.__kV.clearLogVal(uploadId)
        elif self.__cP.get("KV_MODE") == "redis":
            self.__kV.clearLog(fileName)
        dirPath = self.__pathU.getDirPath(repositoryType, depId)
        try:
            tempFile = self.getTempFilePath(uploadId, dirPath)
            os.unlink(tempFile)
        except Exception:
            # tempFile was not found
            pass

    # returns entire dictionary of session table entry
    async def getSession(self, uploadId: str):
        return self.__kV.getKey(uploadId, self.__kV.sessionTable)

    # clear one entry from session table
    async def clearUploadId(self, uid: str):
        response = None
        try:
            response = self.__kV.clearSessionKey(uid)
        except Exception:
            return False
        return response

    # clear one entry from session table and corresponding entry from log table
    def clearSession(self, uid: str, logKey: str):
        response = True
        try:
            res = self.__kV.clearSessionKey(uid)
            if not res:
                response = False
            if self.__cP.get("KV_MODE") == "sqlite":
                self.__kV.clearLogVal(uid)
            elif self.__cP.get("KV_MODE") == "redis":
                self.__kV.clearLog(logKey)
        except Exception:
            return False
        return response

    # clear entire database
    async def clearKv(self):
        self.__kV.clearTable(self.__kV.sessionTable)
        self.__kV.clearTable(self.__kV.logTable)

    # duplicates of upload request functions
    async def getUploadParameters(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        hashDigest: str,
        allowOverwrite: bool,
        resumable: bool,
    ):
        chunkIndex, uploadId = await self.getUploadStatus(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, hashDigest, resumable)
        filePath = await self.getSaveFilePath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite)
        if not filePath:
            raise HTTPException(status_code=400, detail="Error - could not make file path from parameters")
        return filePath, chunkIndex, uploadId

    # return kv entry from file parameters, if have resumed upload, or None if don't
    # if have resumed upload, kv response has chunk count
    async def getUploadStatus(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        hashDigest: str,
        resumable: bool,
    ):
        if version is None or not re.match(r"\d+", version):
            version = await self.findVersion(
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
            uploadId = await self.getResumedUpload(
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
            status = await self.getSession(uploadId)
            if status:
                status = str(status)
                status = status.replace("'", '"')
                status = json.loads(status)
                uploadCount = status["uploadCount"]
        else:
            uploadId = self.getNewUploadId()
        return uploadCount, uploadId

    async def getSaveFilePath(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        allowOverwrite: bool,
    ):
        cP = ConfigProvider()
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

    def getNewUploadId(self):
        return uuid.uuid4().hex
