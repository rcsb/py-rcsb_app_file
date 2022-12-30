##
# File:    IoUtils.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates:
##
"""
Collected I/O utilities.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import asyncio
import datetime
import functools
import gzip
import hashlib
import logging
import os
import shutil
import typing
import uuid
import aiofiles
import re

from fastapi import HTTPException

from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.FileLock import FileLock
from rcsb.app.file.KvSqlite import KvSqlite
from rcsb.app.file.KvRedis import KvRedis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.propagate = False

def wrapAsync(fnc: typing.Callable) -> typing.Awaitable:
    @functools.wraps(fnc)
    def wrap(*args, **kwargs):
        loop = asyncio.get_event_loop()
        func = functools.partial(fnc, *args, **kwargs)
        return loop.run_in_executor(executor=None, func=func)

    return wrap


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
        if self.__cP.get('KV_MODE') == 'sqlite':
            self.__kV = KvSqlite(self.__cP)
        elif self.__cP.get('KV_MODE') == 'redis':
            self.__kV = KvRedis(self.__cP)
        self.__pathU = PathUtils(self.__cP)
        self.__makedirs = wrapAsync(os.makedirs)
        self.__rmtree = wrapAsync(shutil.rmtree)
        self.__replace = wrapAsync(os.replace)
        self.__hashSHA1 = wrapAsync(hashlib.sha1)
        self.__hashMD5 = wrapAsync(hashlib.md5)
        self.__hashSHA256 = wrapAsync(hashlib.sha256)
        self.__checkHashAsync = wrapAsync(self.checkHash)
        self.__getHashDigestAsync = wrapAsync(self.getHashDigest)

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

    # ---
    async def storeUpload(
        self,
        # upload file parameters
        ifh: typing.IO = None,
        uploadId: str = None,
        hashType: str = "MD5",
        hashDigest: typing.Optional[str] = None,
        # optional upload file parameters
        fileName: str = None,
        copyMode: str = "native",  # whether file is a zip file
        mimeType: str = None,
        # chunk parameters
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
        allowOverWrite: bool = True
    ) -> typing.Dict:

        # logger.warning(
        #     "repositoryType %r depId %r contentType %r partNumber %r contentFormat %r version %r copyMode %r", repositoryType, depId, contentType, partNumber, contentFormat, version, copyMode
        # )
        # logger.warning(
        #     "chunk index %s chunk offset %s chunk total %s upload id %s", chunkIndex, chunkOffset, expectedChunks, uploadId
        # )

        # validate parameters
        if not self.__pathU.checkContentTypeFormat(contentType, contentFormat):
            return {"success": False, "statusCode": 405, "statusMessage": "Bad content type and/or format - upload rejected"}

        if fileName and mimeType and not copyMode == "gzip_decompress":
            if fileName.endswith(".gz") or mimeType == "application/gzip":
                copyMode = "gzip_decompress"

        # get path for saving file, test if file exists and is overwritable
        outPath = None
        lockPath = self.__pathU.getFileLockPath(depId, contentType, milestone, partNumber, contentFormat)
        with FileLock(lockPath):
            outPath = self.__pathU.getVersionedPath(repositoryType=repositoryType, depId=depId, contentType=contentType, milestone=milestone, partNumber=partNumber, contentFormat=contentFormat, version=version)
            if not outPath:
                return {"success": False, "statusCode": 405, "statusMessage": "Bad content type metadata - cannot build a valid path"}
            if os.path.exists(outPath) and not allowOverWrite:
                logger.info("Path exists (overwrite %r): %r", allowOverWrite, outPath)
                return {"success": False, "statusCode": 405, "statusMessage": "Encountered existing file - overwrite prohibited"}
            # test if file name in log table (resumed upload or later chunk)
            # if find resumed upload then set uid = previous uid, otherwise new uid
            # lock so that even for concurrent chunks the first chunk will write an upload id that subsequent chunks will use
            if uploadId is None:
                uploadId = await self.getResumedUpload(repositoryType=repositoryType, depId=depId, contentType=contentType, milestone=milestone, partNumber=partNumber, contentFormat=contentFormat, version=version, hashDigest=hashDigest)
                if not uploadId:
                    uploadId = await self.getNewUploadId()
        # versioned path already found, so don't find again
        logKey = self.getPremadeLogKey(repositoryType, outPath)

        # versioned_filename = os.path.basename(outPath)
        # non_versioned_filename = self.__pathU.getBaseFileName(depId, contentType, milestone, partNumber, contentFormat)
        key = uploadId
        val = "uploadCount"
        # initializes to zero
        currentCount = int(self.__kV.getSession(key, val)) # for sequential chunks, current index = current count

        # on first chunk upload, set expected count, record uid in log table
        if currentCount == 0:  # for async, use kv uploadCount rather than parameter chunkIndex == 0:
            self.__kV.setSession(key, "expectedCount", expectedChunks)
            self.__kV.setLog(logKey, uploadId)
            chunksSaved = "0" * expectedChunks
            if chunkMode == 'async':
                self.__kV.setSession(key, "chunksSaved", chunksSaved)
            self.__kV.setSession(key, "timestamp", int(datetime.datetime.timestamp(datetime.datetime.now(datetime.timezone.utc))))
            self.__kV.setSession(key, "hashDigest", hashDigest)
        if chunkMode == 'async':
            chunksSaved = self.__kV.getSession(key, "chunksSaved")
            chunksSaved = list(chunksSaved)
            # do nothing if already have that chunk
            if chunksSaved[chunkIndex] == "1":
                return {"success": True, "statusCode": 200, "uploadId": uploadId, "statusMessage": f"Error - redundant chunk {chunkIndex} of {expectedChunks} for id {uploadId} is less than {currentCount}"}
            # otherwise mark as saved
            chunksSaved[chunkIndex] = "1"  # currentCount
            chunksSaved = "".join(chunksSaved)
            self.__kV.setSession(key, "chunksSaved", chunksSaved)

        ret = None
        if chunkMode == 'sequential':
            ret = await self.sequentialUpload(ifh, outPath, chunkIndex, chunkOffset, expectedChunks, uploadId, key, val, mode="ab", copyMode=copyMode, hashType=hashType, hashDigest=hashDigest, logKey=logKey)
        elif chunkMode == 'async':
            ret = await self.asyncUpload(ifh, outPath, chunkIndex, chunkOffset, expectedChunks, uploadId, key, val, mode="ab", copyMode=copyMode, hashType=hashType, hashDigest=hashDigest, logKey=logKey)
        else:
            return {"success": False, "statusCode": 405,
                    "statusMessage": "error - unknown chunk mode"}

        # if last chunk, clear session, otherwise increment chunk count
        if currentCount + 1 == expectedChunks:
            # session cleared from other function
            pass
        else:
            self.__kV.inc(key, val)

        ret["fileName"] = os.path.basename(outPath) if ret["success"] else None
        ret["uploadId"] = uploadId  # except after last chunk uploadId gets deleted
        return ret

    async def sequentialUpload(
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
        logKey: str = None
    ) -> typing.Dict:
        ok = False
        ret = {"success": False, "statusMessage": None}
        try:
            dirPath, fn = os.path.split(outPath)
            tempPath = self.getTempFilePath(uploadId, dirPath, fn)
            await self.__makedirs(dirPath, mode=0o755, exist_ok=True)

            async with aiofiles.open(tempPath, mode) as ofh:
                await ofh.seek(chunkOffset)
                if copyMode == "native" or copyMode=="shell":
                    await ofh.write(ifh.read())
                    await ofh.flush()
                    os.fsync(ofh.fileno())
                elif copyMode == "gzip_decompress":
                    await ofh.write(gzip.decompress(ifh.read()))
                    await ofh.flush()
                    os.fsync(ofh.fileno())

        except Exception as e:
            logger.exception("Internal write error for path %r: %s", outPath, str(e))
            ret = {"success": False, "statusCode": 400, "statusMessage": f"Store fails with {str(e)}"}
        finally:
            ifh.close()
            ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
            # if last chunk, check hash, finalize
            if (int(self.__kV.getSession(key, val)) + 1) == expectedChunks:
                ok = True
                if hashDigest and hashType:
                    ok = self.checkHash(tempPath, hashDigest, hashType)
                    # ok = await self.__checkHashAsync(tempPath, hashDigest, hashType)
                    if not ok:
                        ret = {"success": False, "statusCode": 400, "statusMessage": f"{hashType} hash check failed"}
                    if ok:
                        await self.__replace(tempPath, outPath)
                        ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
                    if os.path.exists(tempPath):
                        try:
                            os.unlink(tempPath)
                        except Exception:
                            logging.warning("could not delete %s", tempPath)
                else:
                    logging.warning('hash error')
                    ret = {"success": False, "statusCode": 500, "statusMessage": "Error - missing hash"}
                await self.clearSession(key, logKey)
        return ret

    async def asyncUpload(
        self,
        ifh: typing.IO,
        outPath: str,
        chunkIndex: int,
        chunkOffset: int,
        expectedChunks: int,
        uploadId: str,
        key: str,
        val: str,
        mode: typing.Optional[str] = "wb",
        copyMode: typing.Optional[str] = "native",
        hashType: typing.Optional[str] = None,
        hashDigest: typing.Optional[str] = None,
        logKey: str = None
    ) -> typing.Dict:

        ok = False
        ret = {"success": False, "statusMessage": None}
        try:
            dirPath, fn = os.path.split(outPath)
            tempDir = self.getTempDirPath(uploadId, dirPath, fn)
            await self.__makedirs(dirPath, mode=0o755, exist_ok=True)
            await self.__makedirs(tempDir, mode=0o755, exist_ok=True)
            chunkPath = os.path.join(tempDir, str(chunkIndex))
            async with aiofiles.open(chunkPath, mode) as ofh:
                await ofh.seek(chunkOffset)
                if copyMode == "native" or copyMode=="shell":
                    await ofh.write(ifh.read())
                    await ofh.flush()
                    os.fsync(ofh.fileno())
                elif copyMode == "gzip_decompress":
                    await ofh.write(gzip.decompress(ifh.read()))
                    await ofh.flush()
                    os.fsync(ofh.fileno())

        except Exception as e:
            logger.exception("Internal write error for path %r: %s", outPath, str(e))
            ret = {"success": False, "statusCode": 400, "statusMessage": f"Store fails with {str(e)}"}
        finally:
            ifh.close()
            ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
            # if last chunk, check hash, finalize
            if (int(self.__kV.getSession(key, val)) + 1) == expectedChunks:
                tempPath = await self.joinFiles(uploadId, dirPath, fn, tempDir)
                if not tempPath:
                    return {"success": False, "statusCode": 400, "statusMessage": f"error saving {fn}"}
                ok = True
                if hashDigest and hashType:
                    ok = self.checkHash(tempPath, hashDigest, hashType)
                    # ok = await self.__checkHashAsync(tempPath, hashDigest, hashType)
                    if not ok:
                        ret = {"success": False, "statusCode": 400, "statusMessage": f"{hashType} hash check failed"}
                    if ok:
                        await self.__replace(tempPath, outPath)
                        ret = {"success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
                    if os.path.exists(tempPath):
                        try:
                            os.unlink(tempPath)
                        except Exception:
                            logging.warning("could not delete %s", tempPath)
                else:
                    logging.warning('hash error')
                    ret = {"success": False, "statusCode": 500, "statusMessage": "Error - missing hash"}
                await self.clearSession(key, logKey)
        return ret

    async def joinFiles(self, uploadId, dirPath, fn, tempDir):
        tempPath = self.getTempFilePath(uploadId, dirPath, fn)
        try:
            files = sorted([f for f in os.listdir(tempDir) if re.match(r'[0-9]+', f)], key=lambda x: int(x))
            previous = 0
            with open(tempPath, "wb") as w:
                for f in files:
                    index = int(f)
                    if index < previous:
                        raise HTTPException(status_code=500, detail='error - indices not sorted')
                    previous += 1
                    chunkPath = os.path.join(tempDir, f)
                    with open(chunkPath, "rb") as r:
                        w.write(r.read())
                    os.remove(chunkPath)
            os.rmdir(tempDir)
        except Exception:
            return None
        return tempPath

    def getPrimaryLogKey(self,
                         repositoryType: str = "archive",
                         depId: str = None,
                         contentType: str = "model",
                         milestone: str = None,
                         partNumber: int = 1,
                         contentFormat: str = "pdbx",
                         version: str = "next"
                         ):
        # requires lock?
        # filename = self.__pathU.getBaseFileName(depId, contentType, milestone, partNumber, contentFormat)
        filename = self.__pathU.getVersionedPath(repositoryType=repositoryType, depId=depId, contentType=contentType, milestone=milestone, partNumber=partNumber, contentFormat=contentFormat, version=version)
        if not filename:
            return None
        filename = os.path.basename(filename)
        filename = repositoryType + "_" + filename
        return filename

    def getPremadeLogKey(self, repositoryType, versionedPath):
        # does not require lock since versioned path is already found
        if not versionedPath:
            return None
        filename = os.path.basename(versionedPath)
        filename = repositoryType + "_" + filename
        return filename

    # must be different from getTempDirPath
    def getTempFilePath(self, uploadId, dirPath, fileName):
        return os.path.join(dirPath, "." + uploadId)

    # must be different from getTempFilePath
    def getTempDirPath(self, uploadId, dirPath, fileName):
        return os.path.join(dirPath, "._" + uploadId + "_")

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
        # logging.warning(f'get resumed upload version {version} hash {hashDigest}')
        # requires lock?
        filename = self.getPrimaryLogKey(repositoryType=repositoryType, depId=depId, contentType=contentType, milestone=milestone, partNumber=partNumber, contentFormat=contentFormat, version=version)
        uploadId = self.__kV.getLog(filename)
        if not uploadId:
            return None
        # remove expired entries
        timestamp = int(self.__kV.getSession(uploadId, "timestamp"))
        now = datetime.datetime.timestamp(datetime.datetime.now(datetime.timezone.utc))
        duration = now - timestamp
        max_duration = self.__cP.get("KV_MAX_SECONDS")
        if duration > max_duration:
            await self.removeExpiredEntry(uploadId=uploadId, filename=filename, depId=depId, repositoryType=repositoryType)
            return None
        # test if user resumes with same file as previously
        if hashDigest is not None:
            hash = self.__kV.getSession(uploadId, 'hashDigest')
            if hash != hashDigest:
                await self.removeExpiredEntry(uploadId=uploadId, filename=filename, depId=depId, repositoryType=repositoryType)
                return None
        else:
            logging.warning(f'error - no hash')
        return uploadId  # returns uploadId or None

    async def removeExpiredEntry(self,
                                 uploadId: str = None,
                                 fileName: str = None,
                                 depId: str = None,
                                 repositoryType: str = None
                                 ):
        # remove expired entry and temp files
        self.__kV.clearSessionKey(uploadId)
        # still must remove log table entry (key = file parameters)
        self.__kV.clearLog(fileName)
        dirPath = self.__pathU.getDirPath(repositoryType, depId)
        try:
            # don't know which save mode (temp file or temp dir) so remove both
            tempFile = self.getTempFilePath(uploadId, dirPath, fileName)
            os.unlink(tempFile)
            tempDir = self.getTempDirPath(uploadId, dirPath, fileName)
            shutil.rmtree(tempDir, ignore_errors=True)
        except Exception:
            # either tempFile or tempDir was not found
            pass

    async def getNewUploadId(self) -> str:
        return uuid.uuid4().hex

    async def getSession(self, uploadId: str):
        return self.__kV.getKey(uploadId, self.__kV.sessionTable)

    async def clearUploadId(self, uid: str):
        response = None
        try:
            response = self.__kV.clearSessionKey(uid)
        except Exception:
            return False
        return response

    async def clearSession(self, uid: str, logKey: typing.Optional):
        response = True
        try:
            res = self.__kV.clearSessionKey(uid)
            if not res:
                response = False
            if self.__cP.get('KV_MODE') == 'sqlite':
                res = self.__kV.clearLogVal(uid)
            elif self.__cP.get('KV_MODE') == 'redis':
                res = self.__kV.clearLog(logKey)
        except Exception as exc:
            return False
        return response

    async def clearKv(self):
        self.__kV.clearTable(self.__kV.sessionTable)
        self.__kV.clearTable(self.__kV.logTable)

    async def findVersion(self,
                          repositoryType: str = 'archive',
                          depId: str = None,
                          contentType: str = "model",
                          milestone: str = None,
                          partNumber: int = 1,
                          contentFormat: str = "pdbx",
                          version: str = "next"
                          ):
        # requires lock?
        primaryKey = self.getPrimaryLogKey(repositoryType=repositoryType, depId=depId, contentType=contentType, milestone=milestone, partNumber=partNumber, contentFormat=contentFormat, version=version)
        versions = primaryKey.split('.')
        version = versions[-1]
        version = version.replace('V', '')
        return version
