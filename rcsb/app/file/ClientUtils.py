##
# File:    ClientUtils.py
# Author:  Dennis Piehl
# Date:    7-Jun-2022
# Version: 0.001
#
# Updates:
#
##
"""
Client utilities - wrapper of basic functionalities

"""

__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import time
import uuid
import typing
import httpx
# import aiofiles
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ClientUtils():
    """Collected client-side utilities.
    """

    def __init__(self, cachePath=os.environ.get("CACHE_PATH"), configFilePath=os.environ.get("CONFIG_FILE"), hostAndPort=None):
        self.__cachePath = cachePath
        self.__configFilePath = configFilePath
        logger.info("cachePath %s, configFilePath %s", self.__cachePath, self.__configFilePath)
        self.__hostAndPort = hostAndPort if hostAndPort else "http://0.0.0.0:8000"
        logger.info("Server and port address of application: %s", self.__hostAndPort)
        #
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        self.__fU = FileUtil()
        self.__ioU = IoUtils(cP)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath, self.__configFilePath).createToken({}, subject)}
        #

    async def upload(
        self,
        filePath: typing.Optional[str],
        idCode: typing.Optional[str] = None,  # "D_1000000001"
        repositoryType: typing.Optional[str] = None,  # "onedep-archive"
        contentType: typing.Optional[str] = None,  # "model"
        contentFormat: typing.Optional[str] = None,  # "pdbx"
        partNumber: typing.Optional[int] = None,
        version: typing.Optional[str] = None,
        copyMode: typing.Optional[str] = None,  # "native"
        allowOverWrite: typing.Optional[bool] = False,
        hashType: typing.Optional[str] = "MD5",
        hashDigest: typing.Optional[str] = None,
    ):
        """Simple file upload to repository storage or other location"""
        #
        endPoint = "upload"
        #
        #  Using the uncompressed hash
        if copyMode == "decompress_gzip":
            hD = CryptUtils().getFileHash(filePath.split(".gz")[0], hashType=hashType)
        else:
            hD = CryptUtils().getFileHash(filePath, hashType=hashType)
        hashDigest = hashDigest if hashDigest else hD["hashDigest"]
        #
        startTime = time.time()
        try:
            mD = {
                "idCode": idCode,
                "repositoryType": repositoryType,
                "contentType": contentType,
                "contentFormat": contentFormat,
                "partNumber": partNumber,
                "version": str(version),
                "copyMode": copyMode,
                "allowOverWrite": allowOverWrite,
                "hashType": hashType,
                "hashDigest": hashDigest,
            }
            #
            async with httpx.AsyncClient() as client:
                with open(filePath, "rb") as ifh:
                    files = {"uploadFile": ifh}
                    response = await client.post(os.path.join(self.__hostAndPort, "file-v1", endPoint), files=files, data=mD, headers=self.__headerD)
                    logger.info("Uploaded %r with status_code %r", filePath, response.status_code)
                    if response.status_code != 200:
                        logger.error("response %r %r", response.status_code, response.text)
                    rD = response.json()
                    logger.info("rD %r", rD.items())
                    if not rD["success"]:
                        logger.error("response %r %r", response.status_code, response.text)

            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)

    async def multipartUpload(
        self,
        filePath: typing.Optional[str],
        sliceTotal: typing.Optional[int] = None,
        sessionId: typing.Optional[str] = None,
        idCode: typing.Optional[str] = None,  # "D_1000000001"
        repositoryType: typing.Optional[str] = None,  # "onedep-archive"
        contentType: typing.Optional[str] = None,  # "model"
        contentFormat: typing.Optional[str] = None,  # "pdbx"
        partNumber: typing.Optional[int] = None,
        version: typing.Optional[str] = None,
        copyMode: typing.Optional[str] = None,  # "native"
        allowOverWrite: typing.Optional[bool] = False,
        hashType: typing.Optional[str] = "MD5",
        hashDigest: typing.Optional[str] = None,
    ):
        """Multipart sliced-upload to repository storage or other location.
        Begins by first splitting file up into parts, uploading each part, and then joining the parts on the server side.
        """
        endPoint = "upload-slice"
        sessionId = sessionId if sessionId else uuid.uuid4().hex
        try:
            if not sliceTotal:
                sliceSize = 268435456  # 256 MB
                fileSize = self.__fU.size(filePath)
                if fileSize <= sliceSize:
                    sliceTotal = 1
                else:
                    sliceTotal = int(fileSize / sliceSize)
        except Exception as e:
            logger.exception("Failing to determine sliceTotal, with %s", str(e))
        #
        #  Using the uncompressed hash
        if copyMode == "decompress_gzip":
            hD = CryptUtils().getFileHash(filePath.split(".gz")[0], hashType=hashType)
        else:
            hD = CryptUtils().getFileHash(filePath, hashType=hashType)
        hashDigest = hashDigest if hashDigest else hD["hashDigest"]
        #
        # - split the file --
        # First, split the file into slices in a new "sessions" directory (prefixed with "staging");
        # this also creates a "MANIFEST" file containing the names of the file slices.
        sP = await self.__ioU.splitFile(filePath, sliceTotal, "staging" + sessionId, hashType="MD5")
        logger.info("Session path %r", sP)
        #
        manifestPath = os.path.join(sP, "MANIFEST")
        #
        # - upload each slice -
        # Second, read the MANIFEST file to determine what slices there are, and upload each slice using endpoint "upload-slice" to a non-staging "sessions" directory
        # (e.g., if file was split into directory "sessions/stagingX1...", the upload will be placed in adjacent directory "sessions/X1...")
        sliceIndex = 0
        with open(manifestPath, "r", encoding="utf-8") as ifh:
            for line in ifh:
                fn = line[:-1]
                fPath = os.path.join(sP, fn)
                sliceIndex += 1
                startTime = time.time()
                try:
                    mD = {
                        "sliceIndex": sliceIndex,
                        "sliceTotal": sliceTotal,
                        "sessionId": sessionId,
                        "copyMode": copyMode,
                        "allowOverWrite": allowOverWrite,
                        "hashType": None,
                        "hashDigest": None,
                    }
                    #
                    async with httpx.AsyncClient() as client:
                        with open(fPath, "rb") as itfh:
                            files = {"uploadFile": itfh}
                            response = await client.post(os.path.join(self.__hostAndPort, "file-v1", endPoint), files=files, data=mD, headers=self.__headerD)
                        if response.status_code != 200:
                            logger.error("response %r %r", response.status_code, response.text)
                        rD = response.json()
                        logger.debug("rD %r", rD.items())
                        if not rD["success"]:
                            logger.error("response %r %r", response.status_code, response.text)
                    #
                    logger.info("Completed slice (%d) on %s (%.4f seconds)", sliceIndex, endPoint, time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s", str(e))
        #
        # - join the slices -
        # Last, join the slices in the sessions directory together into a single file in the "repository/archive/<idCode>" directory
        endPoint = "join-slice"
        startTime = time.time()
        partNumber = 1
        try:
            mD = {
                "sessionId": sessionId,
                "sliceTotal": sliceTotal,
                "idCode": idCode,
                "repositoryType": repositoryType,
                "contentType": contentType,
                "contentFormat": contentFormat,
                "partNumber": partNumber,
                "version": str(version),
                "copyMode": copyMode,
                "allowOverWrite": allowOverWrite,
                "hashType": hashType,
                "hashDigest": hashDigest,
            }
            #
            async with httpx.AsyncClient() as client:
                with open(fPath, "rb") as ifh:
                    response = await client.post(os.path.join(self.__hostAndPort, "file-v1", endPoint), data=mD, headers=self.__headerD)
                if response.status_code != 200:
                    logger.error("response %r %r", response.status_code, response.text)
                rD = response.json()
                logger.info("rD %r", rD.items())
                if not rD["success"]:
                    logger.error("response %r %r", response.status_code, response.text)
            #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return sessionId

    async def deleteSessionDirectory(self, sessionId: typing.Optional[str]):
        """Delete the staging and main session directories for a given sessionId."""
        # - delete the staging directory -
        ok1 = await self.__ioU.removeSessionDir("staging" + sessionId)
        if not ok1:
            logger.warning("Unable to delete session staging directory for sessionId %s", sessionId)
        #
        # - delete the main session directory -
        ok2 = await self.__ioU.removeSessionDir(sessionId)
        if not ok2:
            logger.warning("Unable to delete session directory for sessionId %s", sessionId)
        #
        return ok1 and ok2

    async def download(
        self,
        fileDownloadPath: typing.Optional[str],  # Location of where to download file
        idCode: typing.Optional[str] = None,  # "D_1000000001"
        repositoryType: typing.Optional[str] = None,  # "onedep-archive"
        contentType: typing.Optional[str] = None,  # "model"
        contentFormat: typing.Optional[str] = None,  # "pdbx"
        partNumber: typing.Optional[int] = None,
        version: typing.Optional[str] = None,
        useHash: typing.Optional[bool] = True,
        hashType: typing.Optional[str] = "MD5",
        hashDigest: typing.Optional[str] = None,
    ):
        """Simple file download from repository storage or other location"""
        #
        endPoint = os.path.join("download", repositoryType)
        #
        startTime = time.time()
        try:
            mD = {
                "idCode": idCode,
                "contentType": contentType,
                "contentFormat": contentFormat,
                "partNumber": partNumber,
                "version": version,
                "hashType": hashType,
            }
            #
            async with httpx.AsyncClient() as client:
                response = await client.get(os.path.join(self.__hostAndPort, "file-v1", endPoint), params=mD, headers=self.__headerD)
                logger.info("download response status code %r", response.status_code)
                if response.status_code != 200:
                    logger.error("response %r %r", response.status_code, response.text)
                logger.info("Content length (%d)", len(response.content))
                if useHash:
                    try:
                        rspHashType = response.headers["rcsb_hash_type"]
                        rspHashDigest = response.headers["rcsb_hexdigest"]
                    except Exception as e:
                        logger.exception("Exception while trying to get hash informatiion from returned response header: %r. Failing with \n%s", response.headers, str(e))
                #
                with open(fileDownloadPath, "wb") as ofh:
                    ofh.write(response.content)
                #
                if useHash:
                    hD = CryptUtils().getFileHash(fileDownloadPath, hashType=hashType)
                    hashDigest = hD["hashDigest"]
                    if hashType != rspHashType:
                        logger.error("Hash type mismatch between requested type %s and returned response type %s", hashType, rspHashType)
                    if hashDigest != rspHashDigest:
                        logger.error(
                            "Hash digest of downloaded file (%s) does not match expected digest from response header (%s) for file %s",
                            hashDigest,
                            rspHashDigest,
                            fileDownloadPath
                        )
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
