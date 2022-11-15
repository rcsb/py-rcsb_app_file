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
        sessionId: int,
        idCode: str,
        repoType: str,
        contentType: str,
        contentFormat: str,
        partNumber: int,
        version: str,
        copyMode: str = "native",
        allowOverWrite: bool = True,
        hashType: str = "MD5",
        hashDigest: typing.Optional[str] = None,
    ) -> typing.Dict:

        logger.debug(
            "repoType %r idCode %r contentType %r partNumber %r contentFormat %r version %r copyMode %r", repoType, idCode, contentType, partNumber, contentFormat, version, copyMode
        )

        if not self.__pathU.checkContentTypeFormat(contentType, contentFormat):
            return {"success": False, "statusCode": 405, "statusMessage": "Bad content type and/or format - upload rejected"}

        # does versioned path require file lock?
        outPath = self.__pathU.getVersionedPath(repoType, idCode, contentType, partNumber, contentFormat, version)
        if not outPath:
            return {"success": False, "statusCode": 405,
                    "statusMessage": "Bad content type metadata - cannot build a valid path"}
        if os.path.exists(outPath) and not allowOverWrite:
            logger.info("Path exists (overwrite %r): %r", allowOverWrite, outPath)
            return {"success": False, "statusCode": 405,
                    "statusMessage": "Encountered existing file - overwrite prohibited"}

        # validate sequential slice index
        filename = os.path.basename(outPath)
        key = str(sessionId)
        val = filename
        currentIndex = self.__kV.get(key, val)  # initializes to zero
        if currentIndex + 1 > sliceTotal:
            return {"success": False, "statusCode": 500,
                    "statusMessage": f"Error - index {sliceIndex} kv index {currentIndex} exceeds expected slice count {sliceTotal}"}
        if sliceIndex < currentIndex:
            return {"success": False, "statusCode": 500,
                    "statusMessage": f"Error - redundant slice {sliceIndex} of {sliceTotal} for id {sessionId} is less than {currentIndex}"}
        if sliceIndex > currentIndex + 1:
            return {"success": False, "statusCode": 500,
                    "statusMessage": f"Error - slice {sliceIndex} of {sliceTotal} is out of order from previous index {currentIndex} for key {key} val {val}"}
            # or busy wait
            # count = 0
            # timeout = 30
            # while sliceIndex > self.__kV.get(key, val) + 1:
            #     await asyncio.sleep(1)
            #     count += 1
            #     if count > timeout:
            #         return {"success": False, "statusCode": 500, "statusMessage": "Error - slices out of order"}

        lockPath = self.__pathU.getFileLockPath(idCode, contentType, partNumber, contentFormat)
        with FileLock(lockPath):
            outPath = self.__pathU.getVersionedPath(repoType, idCode, contentType, partNumber, contentFormat, version)
            if not outPath:
                return {"success": False, "statusCode": 405, "statusMessage": "Bad content type metadata - cannot build a valid path"}
            if os.path.exists(outPath) and not allowOverWrite:
                logger.info("Path exists (overwrite %r): %r", allowOverWrite, outPath)
                return {"success": False, "statusCode": 405, "statusMessage": "Encountered existing file - overwrite prohibited"}

            # logging.warning(f'writing slice {sliceIndex} of {sliceTotal} offset {sliceOffset} file {filename}')
            ret = await self.writeUpload(ifh, outPath, sliceIndex, sliceOffset, sliceTotal, sessionId, key, val, mode="ab", copyMode=copyMode, hashType=hashType, hashDigest=hashDigest)

        if self.__kV.get(key, val) + 1 == sliceTotal:
            self.__kV.clearVal(key, val)
            # what if extra slice arrives after remove...starts a new entry for same file above...how to prevent?
        else:
            self.__kV.inc(key, val)

        ret["fileName"] = os.path.basename(outPath) if ret["success"] else None
        return ret

    async def writeUpload(
        self,
        ifh: typing.IO,
        outPath: str,
        sliceIndex: int,
        sliceOffset: int,
        sliceTotal: int,
        sessionId: int,
        key: str,
        val: str,
        mode: typing.Optional[str] = "wb",
        copyMode: typing.Optional[str] = "native",
        hashType: typing.Optional[str] = None,
        hashDigest: typing.Optional[str] = None,
    ) -> typing.Dict:
        """Store data in the input file handle in the specified output path.

        Args:
            ifh (file-like-object): input file object containing target data
            outPath (str): output file path
            sliceIndex: index of present chunk
            sliceOffset: chunk byte offset
            sliceTotal (int): total number of chunks to be uploaded
            sessionId: unique identifier after login
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
            if (self.__kV.get(key, val) + 1) == sliceTotal:
                # logging.warning(f'{sliceTotal} slices complete')
                ok = True
                if hashDigest and hashType:
                    # ok = self.checkHash(tempPath, hashDigest, hashType)
                    ok = await self.__checkHashAsync(tempPath, hashDigest, hashType)
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

        return ret

    async def getSession(self):
        return uuid.uuid4().hex

    async def clearSession(self, sid: str):
        try:
            self.__kV.clearKey(sid)
        except Exception:
            return False
        return True
