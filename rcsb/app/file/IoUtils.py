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
        lock the idcode/contentType
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
        self.__kV = KvSqlite(self.__cP)
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
        ifh: typing.IO,
        sliceIndex: int,
        sliceOffset: int,
        sliceTotal: int,
        uploadId: str,
        idCode: str,
        repoType: str,
        contentType: str,
        contentFormat: str,
        partNumber: int,
        version: str,
        milestone: str,
        copyMode: str = "native",
        allowOverWrite: bool = True,
        hashType: str = "MD5",
        hashDigest: typing.Optional[str] = None,
        chunkMode: str = "sequential"
    ) -> typing.Dict:

        # logger.warning(
        #     "repoType %r idCode %r contentType %r partNumber %r contentFormat %r version %r copyMode %r", repoType, idCode, contentType, partNumber, contentFormat, version, copyMode
        # )
        # logger.warning(
        #     "slice index %s slice offset %s slice total %s upload id %s", sliceIndex, sliceOffset, sliceTotal, uploadId
        # )

        if not self.__pathU.checkContentTypeFormat(contentType, contentFormat):
            return {"success": False, "statusCode": 405, "statusMessage": "Bad content type and/or format - upload rejected"}

        # get path for saving file, test if file exists and is not overwritable
        outPath = None
        lockPath = self.__pathU.getFileLockPath(idCode, contentType, milestone, partNumber, contentFormat)
        with FileLock(lockPath):
            outPath = self.__pathU.getVersionedPath(repoType, idCode, contentType, milestone, partNumber, contentFormat, version)
            if not outPath:
                return {"success": False, "statusCode": 405, "statusMessage": "Bad content type metadata - cannot build a valid path"}
            if os.path.exists(outPath) and not allowOverWrite:
                logger.info("Path exists (overwrite %r): %r", allowOverWrite, outPath)
                return {"success": False, "statusCode": 405, "statusMessage": "Encountered existing file - overwrite prohibited"}

        # validate sequential slice index
        versioned_filename = os.path.basename(outPath)
        # non_versioned_filename = self.__pathU.getBaseFileName(idCode, contentType, milestone, partNumber, contentFormat)
        key = uploadId
        val = "uploadCount"
        # initializes to zero
        currentCount = self.__kV.getSession(key, val) # for sequential chunks, current index = current count
        # on first chunk upload, set expected count, record uid in log table
        if currentCount == 0:  # for async, use kv uploadCount rather than parameter sliceIndex == 0:
            self.__kV.setSession(key, "expectedCount", sliceTotal)
            pk = self.getPrimaryLogKey(repoType, idCode, contentType, milestone, partNumber, contentFormat, version)
            self.__kV.setLog(pk, uploadId)
            chunksSaved = "0" * sliceTotal
            self.__kV.setSession(key, "chunksSaved", chunksSaved)
        chunksSaved = self.__kV.getSession(key, "chunksSaved")
        chunksSaved = list(chunksSaved)
        # do nothing if already have that chunk
        if chunksSaved[sliceIndex] == "1":
            return {"success": True, "statusCode": 200, "uploadId": uploadId, "statusMessage": f"Error - redundant slice {sliceIndex} of {sliceTotal} for id {uploadId} is less than {currentCount}"}
        # mark as saved
        chunksSaved[sliceIndex] = "1"  # currentCount
        chunksSaved = "".join(chunksSaved)
        self.__kV.setSession(key, "chunksSaved", chunksSaved)

        # if currentCount + 1 > sliceTotal:
        #     return {"success": False, "statusCode": 500,
        #             "statusMessage": f"Error - index {sliceIndex} kv index {currentCount} exceeds expected slice count {sliceTotal}"}
        # if sliceIndex < currentCount:  # resumed upload...already saved
        #     return {"success": True, "statusCode": 200, "uploadId": uploadId,
        #             "statusMessage": f"Error - redundant slice {sliceIndex} of {sliceTotal} for id {uploadId} is less than {currentCount}"}
        # if sliceIndex > currentCount + 1:
        #     return {"success": False, "statusCode": 500,
        #             "statusMessage": f"Error - slice {sliceIndex} of {sliceTotal} is out of order from previous index {currentCount} for key {key} val {val}"}

        ret = None
        if chunkMode == "sequential":
            ret = await self.sequentialUpload(ifh, outPath, sliceIndex, sliceOffset, sliceTotal, uploadId, key, val, mode="ab", copyMode=copyMode, hashType=hashType, hashDigest=hashDigest)
        elif chunkMode in ["async", "asynchronous"]:
            ret = await self.asyncUpload(ifh, outPath, sliceIndex, sliceOffset, sliceTotal, uploadId, key, val, mode="ab", copyMode=copyMode, hashType=hashType, hashDigest=hashDigest)
        else:
            return {"success": False, "statusCode": 405,
                    "statusMessage": "error - unknown chunk mode"}
        # if last slice, clear session, otherwise increment slice count
        if currentCount + 1 == sliceTotal:
            # should clear key except may want to save that for client to enable sessions and session status
            # self.__kV.clearSessionKey(key)
            # what if extra slice arrives after remove...starts a new entry for same file above...how to prevent?
            pass
        else:
            self.__kV.inc(key, val)

        ret["fileName"] = os.path.basename(outPath) if ret["success"] else None
        ret["uploadId"] = uploadId  # except after last slice uploadId could get deleted
        return ret

    async def sequentialUpload(
        self,
        ifh: typing.IO,
        outPath: str,
        sliceIndex: int,
        sliceOffset: int,
        sliceTotal: int,
        uploadId: str,
        key: str,
        val: str,
        mode: typing.Optional[str] = "wb",
        copyMode: typing.Optional[str] = "native",
        hashType: typing.Optional[str] = None,
        hashDigest: typing.Optional[str] = None
    ) -> typing.Dict:
        """Store data in the input file handle in the specified output path.

        Args:
            ifh (file-like-object): input file object containing target data
            outPath (str): output file path
            sliceIndex: index of present chunk
            sliceOffset: chunk byte offset
            sliceTotal (int): total number of chunks to be uploaded
            uploadId: unique identifier after login
            key: database key from previous function
            val: database val from previous function
            mode (str, optional): output file mode
            copyMode (str, optional): concrete copy mode (native|shell). Defaults to 'native'.
            hashType (str, optional): hash type (MD5|SHA1|SHA256). Defaults to 'MD5'.
            hashDigest (str, optional): hash digest. Defaults to None.

        Returns:
            (dict): {"success": True|False, "statusMessage": <text>}
        """
        ok = False
        ret = {"success": False, "statusMessage": None}
        try:
            dirPath, fn = os.path.split(outPath)
            tempPath = os.path.join(dirPath, "." + fn)
            await self.__makedirs(dirPath, mode=0o755, exist_ok=True)

            async with aiofiles.open(tempPath, mode) as ofh:
                await ofh.seek(sliceOffset)
                if copyMode == "native":
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
            ret = {"sliceIndex": sliceIndex, "sliceCount": sliceTotal, "success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
            # if last slice, check hash, finalize
            if (self.__kV.getSession(key, val) + 1) == sliceTotal:
                # logging.warning(f'{sliceTotal} slices complete')
                ok = True
                if hashDigest and hashType:
                    ok = self.checkHash(tempPath, hashDigest, hashType)
                    # ok = await self.__checkHashAsync(tempPath, hashDigest, hashType)
                    if not ok:
                        ret = {"success": False, "statusCode": 400, "statusMessage": f"{hashType} hash check failed"}
                    if ok:
                        await self.__replace(tempPath, outPath)
                        ret = {"sliceIndex": sliceIndex, "sliceCount": sliceTotal, "success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
                        # logging.warning(f'renamed {outPath}')
                        # logger.info("Uploaded %r (%d)", outPath, os.path.getsize(outPath))
                    if os.path.exists(tempPath):
                        try:
                            os.unlink(tempPath)
                            # logging.warning(f'deleted {tempPath}')
                        except Exception:
                            logging.warning("could not delete %s", tempPath)
                else:
                    logging.warning('hash error')
                    ret = {"success": False, "statusCode": 500, "statusMessage": "Error - missing hash"}
                await self.clearSession(key)
        return ret

    async def asyncUpload(
        self,
        ifh: typing.IO,
        outPath: str,
        sliceIndex: int,
        sliceOffset: int,
        sliceTotal: int,
        uploadId: str,
        key: str,
        val: str,
        mode: typing.Optional[str] = "wb",
        copyMode: typing.Optional[str] = "native",
        hashType: typing.Optional[str] = None,
        hashDigest: typing.Optional[str] = None
    ) -> typing.Dict:
        """Store data in the input file handle in the specified output path.

        Args:
            ifh (file-like-object): input file object containing target data
            outPath (str): output file path
            sliceIndex: index of present chunk
            sliceOffset: chunk byte offset
            sliceTotal (int): total number of chunks to be uploaded
            uploadId: unique identifier after login
            key: database key from previous function
            val: database val from previous function
            mode (str, optional): output file mode
            copyMode (str, optional): concrete copy mode (native|shell). Defaults to 'native'.
            hashType (str, optional): hash type (MD5|SHA1|SHA256). Defaults to 'MD5'.
            hashDigest (str, optional): hash digest. Defaults to None.

        Returns:
            (dict): {"success": True|False, "statusMessage": <text>}
        """
        ok = False
        ret = {"success": False, "statusMessage": None}
        try:
            dirPath, fn = os.path.split(outPath)
            tempDir = os.path.join(dirPath, "_" + fn)
            await self.__makedirs(dirPath, mode=0o755, exist_ok=True)
            await self.__makedirs(tempDir, mode=0o755, exist_ok=True)
            chunkPath = os.path.join(tempDir, str(sliceIndex))
            async with aiofiles.open(chunkPath, mode) as ofh:
                await ofh.seek(sliceOffset)
                if copyMode == "native":
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
            ret = {"sliceIndex": sliceIndex, "sliceCount": sliceTotal, "success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
            # if last slice, check hash, finalize
            if (self.__kV.getSession(key, val) + 1) == sliceTotal:
                # logging.warning(f'{sliceTotal} slices complete')
                tempPath = await self.joinFiles(dirPath, fn, tempDir)
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
                        ret = {"sliceIndex": sliceIndex, "sliceCount": sliceTotal, "success": True, "statusCode": 200, "statusMessage": "Store uploaded"}
                        # logging.warning(f'renamed {outPath}')
                        # logger.info("Uploaded %r (%d)", outPath, os.path.getsize(outPath))
                    if os.path.exists(tempPath):
                        try:
                            os.unlink(tempPath)
                            # logging.warning(f'deleted {tempPath}')
                        except Exception:
                            logging.warning("could not delete %s", tempPath)
                else:
                    logging.warning('hash error')
                    ret = {"success": False, "statusCode": 500, "statusMessage": "Error - missing hash"}
                await self.clearSession(key)
        return ret

    async def joinFiles(self, dirPath, fn, tempDir):
        tempPath = os.path.join(dirPath, "." + fn)
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

    def getPrimaryLogKey(self, repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version):
        # filename = self.__pathU.getBaseFileName(idCode, contentType, milestone, partNumber, contentFormat)
        filename = self.__pathU.getVersionedPath(repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version)
        if not filename:
            return None
        filename = os.path.basename(filename)
        filename = repositoryType + "_" + filename
        return filename

    async def getResumedUpload(self, repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version):
        filename = self.getPrimaryLogKey(repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version)
        uploadId = self.__kV.getLog(filename)
        # logging.warning("log for %s = %s", filename, uploadId)
        return uploadId  # returns uploadId or None

    async def getNewUploadId(self) -> str:#, repoType, idCode, contentType, partNumber, contentFormat, version):
        # outPath = self.__pathU.getVersionedPath(repoType, idCode, contentType, str(partNumber), contentFormat, str(version))
        # if not outPath:
        #     return None
        # return os.path.basename(outPath)
        return uuid.uuid4().hex

    async def findUploadId(self, repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version) -> typing.Optional[str]:
        filename = self.getPrimaryLogKey(repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version)
        uploadId = self.__kV.getLog(filename)
        return None if not uploadId else uploadId

    async def getSession(self, uploadId: str):
        return self.__kV.getKey(uploadId, self.__kV.sessionTable)

    async def clearUploadId(self, uid: str):
        response = None
        try:
            response = self.__kV.clearSessionKey(uid)
        except Exception:
            return False
        return response

    # async def uploadStatus(self, uploadId: str):
    #     return self.__kV.getKey(uploadId, self.__kV.sessionTable)

    # not yet implemented, supposed to have sessionId rather than uploadId
    # async def uploadStatuses(self, uploadIds: list) -> list:
    #     return [self.__kV.getKey(uid, self.__kV.sessionTable) for uid in uploadIds]

    async def clearSession(self, uid: str):
        response = True
        try:
            res = self.__kV.clearSessionKey(uid)
            if not res:
                response = False
            res = self.__kV.clearLogVal(uid)
        except Exception as exc:
            return False
        return response

    async def clearSessions(self, uploadIds: list):
        response = True
        try:
            for uid in uploadIds:
                res = self.__kV.clearSessionKey(uid)
                if not res:
                    response = False
                res = self.__kV.clearLogVal(uid)
        except Exception as exc:
            return False
        return response

    async def clearKv(self):
        self.__kV.clearTable(self.__kV.sessionTable)
        self.__kV.clearTable(self.__kV.logTable)

    async def findVersion(self, repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version):
        primaryKey = self.getPrimaryLogKey(repositoryType, idCode, contentType, milestone, partNumber, contentFormat, version)
        versions = primaryKey.split('.')
        version = versions[-1]
        version = version.replace('V', '')
        return version
