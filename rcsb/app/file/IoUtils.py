##
# File:    IoUtils.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates: James Smith 2023
#
"""
Collected I/O utilities.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

# Tasks:
# Improve email service

import datetime
import gzip
import hashlib
import logging
import os
import shutil
import typing
import uuid
import aiofiles
import re
import requests
from filelock import Timeout, FileLock
from fastapi import HTTPException
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathUtils import PathUtils
from rcsb.app.file.KvSqlite import KvSqlite
from rcsb.app.file.KvRedis import KvRedis
# import asyncio
# import functools

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


# def wrapAsync(fnc: typing.Callable) -> typing.Awaitable:
#     @functools.wraps(fnc)
#     def wrap(*args, **kwargs):
#         loop = asyncio.get_event_loop()
#         func = functools.partial(fnc, *args, **kwargs)
#         return loop.run_in_executor(executor=None, func=func)
#
#     return wrap


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
        # self.__makedirs = wrapAsync(os.makedirs)
        # self.__rmtree = wrapAsync(shutil.rmtree)
        # self.__replace = wrapAsync(os.replace)
        # self.__hashSHA1 = wrapAsync(hashlib.sha1)
        # self.__hashMD5 = wrapAsync(hashlib.md5)
        # self.__hashSHA256 = wrapAsync(hashlib.sha256)
        # self.__checkHashAsync = wrapAsync(self.checkHash)
        # self.__getHashDigestAsync = wrapAsync(self.getHashDigest)

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
                # hashObj = await self.__hashSHA1()
                hashObj = hashlib.sha1()
            elif hashType == "SHA256":
                # hashObj = await self.__hashSHA256()
                hashObj = hashlib.sha256()
            elif hashType == "MD5":
                # hashObj = await self.__hashMD5()
                hashObj = hashlib.md5()

            with open(filePath, "rb") as ifh:
                for block in iter(lambda: ifh.read(blockSize), b""):
                    hashObj.update(block)
            return hashObj.hexdigest()
        except Exception as e:
            logger.exception("Failing with file %s %r", filePath, str(e))
        return None

    # upload complete file
    async def upload(
            self,
            # upload file parameters
            uploadFile: typing.IO,
            uploadId: str,
            hashType: str,
            hashDigest: str,
            # save file parameters
            depId: str,
            repositoryType: str,
            contentType: str,
            milestone: str,
            partNumber: int,
            contentFormat: str,
            version: str,
            copyMode: str,
            allowOverwrite: bool
    ):
        tempPath = None
        outPath = None
        ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
        try:
            cachePath = os.environ.get("CACHE_PATH")
            configFilePath = os.environ.get("CONFIG_FILE")
            cP = ConfigProvider(cachePath, configFilePath)
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
            dirPath, fn = os.path.split(outPath)
            uploadId = self.getNewUploadId()
            tempPath = os.path.join(dirPath, "." + uploadId)
            os.makedirs(dirPath, mode=0o755, exist_ok=True)
            # save (all copy modes), then hash, then decompress
            with open(tempPath, "wb") as ofh:
                shutil.copyfileobj(uploadFile, ofh)
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
                    raise HTTPException(status_code=400,
                                        detail=f"{hashType} hash check failed {hexdigest} != {hashDigest}")
                else:
                    # lock before saving
                    lockPath = os.path.join(os.path.dirname(outPath), "." + os.path.basename(outPath) + ".lock")
                    lock = FileLock(lockPath)
                    try:
                        with lock.acquire(timeout=60 * 60 * 4):
                            # last minute race condition prevention
                            if os.path.exists(outPath) and not allowOverwrite:
                                raise HTTPException(status_code=400,
                                                    detail="Encountered existing file - cannot overwrite")
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
            uploadFile.close()
        if not ret["success"]:
            raise HTTPException(status_code=405, detail=ret["statusMessage"])
        return ret

    # in-place sequential chunk
    async def sequentialUpload(
            self,
            # upload file parameters
            uploadFile: typing.IO,
            uploadId: str,
            hashType: str,
            hashDigest: str,
            # chunk parameters
            chunkSize: int,
            chunkIndex: int,
            chunkOffset: int,
            expectedChunks: int,
            chunkMode: str,
            # save file parameters
            filePath: str,
            copyMode: str,
            allowOverwrite: bool
    ):
        ret = {"success": True, "statusCode": 200, "statusMessage": "Chunk uploaded"}
        dirPath, fn = os.path.split(filePath)
        tempPath = os.path.join(dirPath, "." + uploadId)
        try:
            # save, then hash, then decompress
            # should lock, however client must wait for each response before sending next chunk, precluding race conditions (unless multifile upload problem)
            async with aiofiles.open(tempPath, "ab") as ofh:
                await ofh.seek(chunkOffset)
                await ofh.write(uploadFile.read())
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
                                    raise HTTPException(status_code=400,
                                                        detail="Encountered existing file - cannot overwrite")
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
            ret = {"success": False, "statusCode": exc.status_code,
                   "statusMessage": f"error in sequential upload {exc.detail}"}
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        except Exception as exc:
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            ret = {"success": False, "statusCode": 400, "statusMessage": f"error in sequential upload {str(exc)}"}
            raise HTTPException(status_code=400, detail=f'error in sequential upload {str(exc)}')
        return ret

    # resumable chunked upload with either async or sequential chunks
    async def resumableUpload(
        self,
        # upload file parameters
        ifh: typing.IO = None,
        uploadId: str = None,
        hashType: str = "MD5",
        hashDigest: typing.Optional[str] = None,
        # optional upload file parameters
        fileName: str = None,
        mimeType: str = None,
        # chunk parameters
        chunkSize: int = 0,
        chunkIndex: int = 0,
        chunkOffset: int = 0,
        expectedChunks: int = 1,
        chunkMode: str = "sequential",
        # save file parameters
        depId: str = None,
        repositoryType: str = "archive",
        contentType: str = "model",
        milestone: str = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
        version: str = "next",
        allowOverwrite: bool = True,
        copyMode: str = "native",  # whether file is a zip file
        emailAddress: str = None
    ):

        # validate parameters
        if not self.__pathU.checkContentTypeFormat(contentType, contentFormat):
            raise HTTPException(status_code=400, detail="Bad content type and/or format - upload rejected")

        # get path for saving file, test if file exists and is overwritable
        outPath = None
        # could change when have race condition, resulting in different names for different chunks of same file, stopped by hash check prior to saving
        outPath = self.__pathU.getVersionedPath(
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
            logger.info("Path exists (overwrite %r): %r", allowOverwrite, outPath)
            raise HTTPException(status_code=400, detail="Encountered existing file - overwrite prohibited")
        # test if file name in log table (resumed upload or later chunk)
        # if find resumed upload then set uid = previous uid, otherwise new uid
        # lock so that even for concurrent chunks the first chunk will write an upload id that subsequent chunks will use
        if uploadId is None:
            uploadId = await self.getResumedUpload(
                repositoryType=repositoryType,
                depId=depId, contentType=contentType,
                milestone=milestone,
                partNumber=partNumber,
                contentFormat=contentFormat,
                version=version,
                hashDigest=hashDigest
            )
            if not uploadId:
                uploadId = self.getNewUploadId()

        # versioned path already found, so don't find again
        logKey = self.getPremadeLogKey(repositoryType, outPath)
        key = uploadId
        val = "uploadCount"
        # initializes to zero
        currentCount = int(self.__kV.getSession(key, val))  # for sequential chunks, current index = current count

        # on first chunk upload, set expected count, record uid in log table
        if currentCount == 0:  # for async, use kv uploadCount rather than parameter chunkIndex == 0:
            self.__kV.setSession(key, "expectedCount", expectedChunks)
            self.__kV.setLog(logKey, uploadId)
            self.__kV.setSession(key, "timestamp", int(datetime.datetime.timestamp(datetime.datetime.now(datetime.timezone.utc))))
            self.__kV.setSession(key, "hashDigest", hashDigest)
            # for async mode and return value expected from get session
            chunksSaved = "0" * expectedChunks
            self.__kV.setSession(key, "chunksSaved", chunksSaved)
        if chunkMode == "async":
            chunksSaved = self.__kV.getSession(key, "chunksSaved")
            chunksSaved = list(chunksSaved)
            # do nothing if already have that chunk
            if chunksSaved[chunkIndex] == "1":
                return {
                    "success": True,
                    "statusCode": 200,
                    "uploadId": uploadId,
                    "statusMessage": f"Error - redundant chunk {chunkIndex} of {expectedChunks} for id {uploadId} is less than {currentCount}"
                }
            # otherwise mark as saved
            chunksSaved[chunkIndex] = "1"  # currentCount
            chunksSaved = "".join(chunksSaved)
            self.__kV.setSession(key, "chunksSaved", chunksSaved)

        ret = None
        if chunkMode == "sequential":
            self.__kV.inc(key, val)
            ret = self.sequentialChunk(
                ifh,
                outPath,
                chunkIndex,
                chunkOffset,
                expectedChunks,
                uploadId,
                key,
                val,
                mode="ab",
                copyMode=copyMode,
                hashType=hashType,
                hashDigest=hashDigest,
                logKey=logKey,
                allowOverwrite=allowOverwrite
            )
            ret["fileName"] = os.path.basename(outPath) if ret["success"] else None
            ret["uploadId"] = uploadId  # except after last chunk uploadId gets deleted
            return ret
        elif chunkMode == "async":
            self.asyncChunk(
                ifh,
                outPath,
                chunkIndex,
                chunkOffset,
                expectedChunks,
                chunkSize,
                uploadId,
                key,
                val,
                "ab",
                copyMode,
                hashType,
                hashDigest,
                logKey,
                emailAddress,
                allowOverwrite
            )
            if chunkIndex + 1 == expectedChunks:
                self.syncFiles(outPath,
                    chunkIndex,
                    chunkOffset,
                    expectedChunks,
                    chunkSize,
                    uploadId,
                    key,
                    val,
                    "ab",
                    copyMode,
                    hashType,
                    hashDigest,
                    logKey,
                    emailAddress,
                    allowOverwrite)
            ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
            return ret
        else:
            raise HTTPException(status_code=400, detail="error - unknown chunk mode")

    # in-place resumable sequential chunk

    def sequentialChunk(
        self,
        ifh: typing.IO,
        outPath: str,
        chunkIndex: int,
        chunkOffset: int,
        expectedChunks: int,
        uploadId: str,
        key: str,
        val: str,
        mode: typing.Optional[str] = "ab",
        copyMode: typing.Optional[str] = "native",
        hashType: typing.Optional[str] = None,
        hashDigest: typing.Optional[str] = None,
        logKey: str = None,
        emailAddress: str = None,
        allowOverwrite: bool = None
    ) -> typing.Dict:

        ok = False
        ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
        tempPath = None
        try:
            dirPath, fn = os.path.split(outPath)
            tempPath = self.getTempFilePath(uploadId, dirPath)
            os.makedirs(dirPath, mode=0o755, exist_ok=True)
            # save, then hash, then decompress
            # should lock, however client must wait for each response before sending next chunk, precluding race conditions (unless multifile upload problem)
            with open(tempPath, mode) as ofh:
                ofh.seek(chunkOffset)
                ofh.write(ifh.read())
                ofh.flush()
                os.fsync(ofh.fileno())
            # if last chunk, check hash, finalize
            if (int(self.__kV.getSession(key, val))) == expectedChunks:
                ok = True
                if hashDigest and hashType:
                    # hash
                    ok = self.checkHash(tempPath, hashDigest, hashType)
                    if not ok:
                        raise HTTPException(status_code=400, detail=f"{hashType} hash check failed")
                    else:
                        # lock then save
                        lockPath = os.path.join(os.path.dirname(outPath), "." + os.path.basename(outPath) + ".lock")
                        lock = FileLock(lockPath)
                        try:
                            with lock.acquire(timeout=60 * 60 * 4):
                                # last minute race condition handling
                                if os.path.exists(outPath) and not allowOverwrite:
                                    raise HTTPException(status_code=400, detail='error - encountered existing file without overwrite')
                                else:
                                    # save final version
                                    os.replace(tempPath, outPath)
                                    ret = {"success": True, "statusCode": 200, "statusMessage": f"Store uploaded for {fn}"}
                                    msg = ret["statusMessage"]
                                    response = self.sendEmail(emailAddress, msg)
                                    if response:
                                        ret["statusMessage"] = response
                        except Timeout:
                            raise HTTPException(status_code=400, detail=f'error - lock timed out on {outPath}')
                        finally:
                            lock.release()
                            if os.path.exists(lockPath):
                                os.unlink(lockPath)
                    if os.path.exists(tempPath):
                        os.unlink(tempPath)
                    # decompress
                    if copyMode == "gzip_decompress":
                        # just deleted temp path but using again for a temp file
                        with gzip.open(outPath, "rb") as r:
                            with open(tempPath, "wb") as w:
                                w.write(r.read())
                        os.replace(tempPath, outPath)
                else:
                    logging.warning("hash error")
                    raise HTTPException(status_code=400, detail="Error - missing hash")
                self.clearSession(key, logKey)
        except HTTPException as exc:
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            ret = {"success": False, "statusCode": exc.status_code, "statusMessage": exc.detail}
            logger.exception("Internal write error for path %r: %s", outPath, exc.detail)
            raise HTTPException(status_code=exc.status_code, detail=f"Store fails with {exc.detail}")
        except Exception as exc:
            if os.path.exists(tempPath):
                os.unlink(tempPath)
            ret = {"success": False, "statusCode": 400, "statusMessage": str(exc)}
            logger.exception("Internal write error for path %r: %s", outPath, str(exc))
            raise HTTPException(status_code=400, detail=f"Store fails with {str(exc)}")
        finally:
            ifh.close()
        return ret

    # resumable distributed chunk with joining
    def asyncChunk(
        self,
        ifh: typing.IO,
        outPath: str,
        chunkIndex: int,
        chunkOffset: int,
        expectedChunks: int,
        chunkSize: int,
        uploadId: str,
        key: str,
        val: str,
        mode: typing.Optional[str] = "wb",
        copyMode: typing.Optional[str] = "native",
        hashType: typing.Optional[str] = None,
        hashDigest: typing.Optional[str] = None,
        logKey: str = None,
        emailAddress: str = None,
        allowOverwrite: bool = False
    ) -> typing.Dict:
        tempDir = None
        tempPath = None
        fn = None
        ret = {"success": True, "statusCode": 200, "statusMessage": f"Store uploaded for {fn}"}
        try:
            dirPath, fn = os.path.split(outPath)
            tempDir = self.getTempDirPath(uploadId, dirPath)
            os.makedirs(dirPath, mode=0o755, exist_ok=True)
            os.makedirs(tempDir, mode=0o755, exist_ok=True)
            chunkPath = os.path.join(tempDir, str(chunkIndex))
            with open(chunkPath, mode) as ofh:
                ofh.seek(chunkOffset)
                ofh.write(ifh.read())
                ofh.flush()
                os.fsync(ofh.fileno())
            self.__kV.inc(key, val)
        except HTTPException as exc:
            if tempPath and os.path.exists(tempPath):
                os.unlink(tempPath)
            logger.exception("Internal write error for path %r: %s", outPath, exc.detail)
            ret = {"success": False, "statusCode": exc.status_code, "statusMessage": exc.detail}
            raise HTTPException(status_code=exc.status_code, detail=f"Store fails with {exc.detail}")
        except Exception as exc:
            if tempPath and os.path.exists(tempPath):
                os.unlink(tempPath)
            logger.exception("Internal write error for path %r: %s", outPath, str(exc))
            ret = {"success": False, "statusCode": 400, "statusMessage": str(exc)}
            raise HTTPException(status_code=400, detail=f"Store fails with {str(exc)}")
        finally:
            ifh.close()
        return ret

    # invoke join chunks
    def syncFiles(
            self,
            outPath: str,
            chunkIndex: int,
            chunkOffset: int,
            expectedChunks: int,
            chunkSize: int,
            uploadId: str,
            key: str,
            val: str,
            mode: typing.Optional[str] = "wb",
            copyMode: typing.Optional[str] = "native",
            hashType: typing.Optional[str] = None,
            hashDigest: typing.Optional[str] = None,
            logKey: str = None,
            emailAddress: str = None,
            allowOverwrite: bool = False
    ) -> typing.Dict:
        ok = False
        tempDir = None
        tempPath = None
        fn = None
        ret = {"success": True, "statusCode": 200, "statusMessage": f"Store uploaded for {fn}"}
        try:
            dirPath, fn = os.path.split(outPath)
            tempDir = self.getTempDirPath(uploadId, dirPath)
            os.makedirs(dirPath, mode=0o755, exist_ok=True)
            os.makedirs(tempDir, mode=0o755, exist_ok=True)
            # chunkPath = os.path.join(tempDir, str(chunkIndex))
            ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
            tempPath = self.joinChunks(uploadId, dirPath, fn, tempDir, chunkSize)
            if not tempPath:
                raise HTTPException(status_code=400, detail=f"error saving {fn}")
            else:
                ok = True
                if hashDigest and hashType:
                    # hash
                    ok = self.checkHash(tempPath, hashDigest, hashType)
                    if not ok:
                        raise HTTPException(status_code=400, detail=f"{hashType} hash check failed")
                    else:
                        # lock then save
                        lockPath = os.path.join(os.path.dirname(outPath),
                                                "." + os.path.basename(outPath) + ".lock")
                        lock = FileLock(lockPath)
                        try:
                            with lock.acquire(timeout=60 * 60 * 4):
                                # last minute race condition handling
                                if os.path.exists(outPath) and not allowOverwrite:
                                    raise HTTPException(status_code=400,
                                                        detail='error - encountered existing file without overwrite')
                                else:
                                    # save final version
                                    os.replace(tempPath, outPath)
                        except Timeout:
                            raise HTTPException(status_code=400, detail=f'error - lock timed out on {outPath}')
                        finally:
                            lock.release()
                            if os.path.exists(lockPath):
                                os.unlink(lockPath)
                        msg = ret["statusMessage"]
                        response = self.sendEmail(emailAddress, msg)
                        if response:
                            ret["statusMessage"] = response
                    if os.path.exists(tempPath):
                        os.unlink(tempPath)
                    # decompress
                    if copyMode == "gzip_decompress":
                        # just deleted temp path but using again for a temp file
                        with gzip.open(outPath, "rb") as r:
                            with open(tempPath, "wb") as w:
                                w.write(r.read())
                        os.replace(tempPath, outPath)
                else:
                    logging.warning("hash error")
                    raise HTTPException(status_code=400, detail="Error - missing hash")
            self.clearSession(key, logKey)
        except HTTPException as exc:
            if tempPath and os.path.exists(tempPath):
                os.unlink(tempPath)
            logger.exception("Internal write error for path %r: %s", outPath, exc.detail)
            ret = {"success": False, "statusCode": exc.status_code, "statusMessage": exc.detail}
            raise HTTPException(status_code=exc.status_code, detail=f"Store fails with {exc.detail}")
        except Exception as exc:
            if tempPath and os.path.exists(tempPath):
                os.unlink(tempPath)
            logger.exception("Internal write error for path %r: %s", outPath, str(exc))
            ret = {"success": False, "statusCode": 400, "statusMessage": str(exc)}
            raise HTTPException(status_code=400, detail=f"Store fails with {str(exc)}")
        return ret

    # join chunks into one file

    def joinChunks(self, uploadId, dirPath, fn, tempDir, chunkSize):
        tempPath = self.getTempFilePath(uploadId, dirPath)
        try:
            files = sorted([f for f in os.listdir(tempDir) if re.match(r"[0-9]+", f)], key=lambda x: int(x))
            previous = 0
            with open(tempPath, "wb") as w:
                for f in files:
                    index = int(f)
                    if index < previous:
                        raise HTTPException(status_code=500, detail="error - indices not sorted")
                    previous += 1
                    chunkPath = os.path.join(tempDir, f)
                    with open(chunkPath, "rb") as r:
                        w.write(r.read())
                    os.unlink(chunkPath)
        except HTTPException:
            tempPath = None
        except Exception:
            tempPath = None
        finally:
            try:
                os.rmdir(tempDir)
            except Exception:
                logging.warning(f'error - could not delete {tempDir}')
                tempPath = None
        return tempPath

    """
    utility functions
    """
    def getNewUploadId(self) -> str:
        return uuid.uuid4().hex

    # please change to legitimate email service
    def sendEmail(self,
                emailAddress: str = None,
                msg: str = None
                ):
        if not emailAddress or not msg:
            return None
        url = "https://springbootemailwebservice.000webhostapp.com"
        body = {
            "sender": "PDB",
            "subject": "one-dep upload status",
            "recipient": emailAddress,
            "body": msg
        }
        response = requests.post(url, data=body)
        if response.status_code == 200:
            return response.text
        else:
            logging.warning(f'email response {response.status_code}')
            return response.status_code

    """
    file path functions
    """

    async def getSaveFilePath(self,
                              repositoryType: str,
                              depId: str,
                              contentType: str,
                              milestone: typing.Optional[str],
                              partNumber: int,
                              contentFormat: str,
                              version: str,
                              allowOverwrite: bool):
        if not self.__pathU.checkContentTypeFormat(contentType, contentFormat):
            raise HTTPException(status_code=400, detail="Bad content type and/or format - upload rejected")
        outPath = self.__pathU.getVersionedPath(
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
        return outPath

    # must be different from getTempDirPath
    def getTempFilePath(self, uploadId, dirPath):
        return os.path.join(dirPath, "." + uploadId)

    # must be different from getTempFilePath
    def getTempDirPath(self, uploadId, dirPath):
        return os.path.join(dirPath, "._" + uploadId + "_")

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

    """
    database functions
    """

    # return kv entry from file parameters, if have resumed upload, or None if don't
    # if have resumed upload, kv response has chunk indices and count
    async def getUploadStatus(self,
                              repositoryType: str,
                              depId: str,
                              contentType: str,
                              milestone: typing.Optional[str],
                              partNumber: int,
                              contentFormat: str,
                              version: str,
                              hashDigest: str):
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
        if not uploadId:
            return None
        status = await self.getSession(uploadId)
        return status

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

    # remove an entry from session table and log table, remove corresponding hidden files and folders
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
            # don't know which save mode (temp file or temp dir) so remove both
            tempFile = self.getTempFilePath(uploadId, dirPath)
            os.unlink(tempFile)
            tempDir = self.getTempDirPath(uploadId, dirPath)
            shutil.rmtree(tempDir, ignore_errors=True)
        except Exception:
            # either tempFile or tempDir was not found
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
    def clearSession(self, uid: str, logKey: typing.Optional):
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
