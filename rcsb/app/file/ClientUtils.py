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
import typing
import asyncio
import io
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import httpx
import requests
import json
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


class ClientUtils:
    """Collected client-side utilities."""

    def __init__(
        self,
        cachePath=os.environ.get("CACHE_PATH"),
        configFilePath=os.environ.get("CONFIG_FILE"),
        hostAndPort=None,
    ):
        self.__cachePath = cachePath
        self.__configFilePath = configFilePath
        logger.info(
            "cachePath %s, configFilePath %s", self.__cachePath, self.__configFilePath
        )
        self.__hostAndPort = hostAndPort if hostAndPort else "http://0.0.0.0:8000"
        logger.info("Server and port address of application: %s", self.__hostAndPort)
        #
        self.__timeout = None  # Turn off request timeout
        #
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        self.__fU = FileUtil()
        self.__ioU = IoUtils(cP)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {
            "Authorization": "Bearer "
            + JWTAuthToken(self.__cachePath, self.__configFilePath).createToken(
                {}, subject
            )
        }
        #

    """ upload multiple files with async or concurrent behavior
        one file is treated as a subset of multiple files
    """

    async def upload(self, data: list) -> list:
        # data is a list of dictionaries, return value is a list of multi-file results
        # for one file upload, returns a list of size 1
        tasks = []
        for _d in data:
            tasks.append(self.uploadFile(**_d))
        return await asyncio.gather(*tasks)

    """ requires Python >= 3.11 
        couldn't install mmcif after switching interpreter to 3.11
    """
    # async def uploadv2(self, data: list):
    #     # data is a list of dictionaries
    #     try:
    #         tasks = []
    #         async with asyncio.TaskGroup() as group:
    #             for _d in data:
    #                 tasks.append(group.create_task(self.uploadFile(**_d)))
    #         return [t.result() for t in tasks]
    #     except Exception as exc:
    #         logging.warning(f'error in upload {type(exc)} {exc}')
    #         return None

    """ has slight problem with return values """

    async def uploadv3(self, data: list):
        # data is a list of dictionaries
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for _d in data:
                results.append(
                    executor.submit(await self.uploadFile(**_d))
                )  # not supposed to put *d in parentheses, throws type error, but otherwise await throws worse error
        # logging.info(results)
        # can't print results since mixed async thread results have type error
        return results

    """ multipart upload of one file 
        chunks must be uploaded sequentially rather than concurrently or asynchronously
        to ensure sequential behavior, must wait for a response before uploading next chunk
    """

    async def uploadFile(
        self,
        filePath: str,
        idCode: str,
        repositoryType: str,
        contentType: str,
        contentFormat: str,
        partNumber: int,
        version: str,
        copyMode: str,
        allowOverWrite: bool,
        sliceSize: int = None,
        uploadId: str = None,
    ):

        # print(f'upload {idCode} part {partNumber} path {filePath}')

        endpoint = "upload"
        url = os.path.join(self.__hostAndPort, "file-v2", endpoint)

        hashType = "MD5"
        hD = CryptUtils().getFileHash(filePath, hashType=hashType)
        fullTestHash = hD["hashDigest"]

        if sliceSize is None:
            sliceSize = 1024 * 1024 * 8
        fileSize = os.path.getsize(filePath)
        sliceTotal = 0
        if sliceSize < fileSize:
            sliceTotal = fileSize // sliceSize
            if fileSize % sliceSize:
                sliceTotal = sliceTotal + 1
        else:
            sliceTotal = 1
        sliceIndex = 0
        sliceOffset = 0

        # if uploadId is None:
            # print("creating new upload id")
            # uploadId = await self.getNewUploadId()#repositoryType, idCode, contentType, partNumber, contentFormat, version)

        mD = {
            "sliceIndex": sliceIndex,
            "sliceOffset": sliceOffset,
            "sliceTotal": sliceTotal,
            "uploadId": uploadId,
            "idCode": idCode,
            "repositoryType": repositoryType,
            "contentType": contentType,
            "contentFormat": contentFormat,
            "partNumber": partNumber,
            "version": str(version),
            "copyMode": copyMode,
            "allowOverWrite": allowOverWrite,
            "hashType": hashType,
            "hashDigest": fullTestHash,
        }

        response = None
        tmp = io.BytesIO()
        try:
            with open(filePath, "rb") as upLoad:
                for i in range(0, sliceTotal):
                    packetSize = min(
                        fileSize - (mD["sliceIndex"] * sliceSize),
                        sliceSize,
                    )
                    tmp.truncate(packetSize)
                    tmp.seek(0)
                    tmp.write(upLoad.read(packetSize))
                    tmp.seek(0)
                    response = requests.post(
                        url,
                        data=deepcopy(mD),
                        headers=self.__headerD,
                        files={"uploadFile": tmp},
                        timeout=self.__timeout,
                    )
                    if response.status_code != 200:
                        print(
                            f"error - status code {response.status_code} {response.text} url {url} file {filePath}"
                        )
                        break
                    mD["sliceIndex"] += 1
                    mD["sliceOffset"] = mD["sliceIndex"] * sliceSize
                    # time.sleep(1)
                    # text = json.loads(response.text)
                    # mD["uploadId"] = text["uploadId"]
        except asyncio.CancelledError as exc:
            logger.exception("error in sliced upload %s", exc)

        # return response from last slice uploaded (if all slices were uploaded)
        return None if not response else response

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
            async with httpx.AsyncClient(timeout=self.__timeout) as client:
                # Default timeout is 5.0 seconds, but takes ~120 seconds for ~3 GB file
                response = await client.get(
                    os.path.join(self.__hostAndPort, "file-v1", endPoint),
                    params=mD,
                    headers=self.__headerD,
                )
                logger.info("download response status code %r", response.status_code)
                if response.status_code != 200:
                    logger.error("response %r %r", response.status_code, response.text)
                logger.info("Content length (%d)", len(response.content))
                if useHash:
                    try:
                        rspHashType = response.headers["rcsb_hash_type"]
                        rspHashDigest = response.headers["rcsb_hexdigest"]
                    except Exception as e:
                        logger.exception(
                            "Exception while trying to get hash informatiion from returned response header: %r. Failing with \n%s",
                            response.headers,
                            str(e),
                        )
                #
                with open(fileDownloadPath, "wb") as ofh:
                    ofh.write(response.content)
                #
                if useHash:
                    hD = CryptUtils().getFileHash(fileDownloadPath, hashType=hashType)
                    hashDigest = hD["hashDigest"]
                    if hashType != rspHashType:
                        logger.error(
                            "Hash type mismatch between requested type %s and returned response type %s",
                            hashType,
                            rspHashType,
                        )
                    if hashDigest != rspHashDigest:
                        logger.error(
                            "Hash digest of downloaded file (%s) does not match expected digest from response header (%s) for file %s",
                            hashDigest,
                            rspHashDigest,
                            fileDownloadPath,
                        )
                #
            logger.info(
                "Completed %s (%.4f seconds)", endPoint, time.time() - startTime
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))

    async def getNewUploadId(self):#, repositoryType, idCode, contentType, partNumber, contentFormat, version):
        return await self.__ioU.getNewUploadId()#repositoryType, idCode, contentType, partNumber, contentFormat, version)

    async def clearUploadId(self, uid):
        url = os.path.join(self.__hostAndPort, "file-v2", "clearUploadId")
        response = requests.post(url, data={"uid": uid}, headers=self.__headerD, timeout=None)
        # ok = await self.__ioU.clearUploadId(uid)
        if response.status_code != 200:
            logging.warning("error - could not delete upload id %s", uid)
            return False
        return True

    async def clearSession(self, uploadIds: list):
        url = os.path.join(self.__hostAndPort, "file-v2", "clearSession")
        response = requests.post(url, data={"uploadIds": uploadIds}, headers=self.__headerD, timeout=None)
        # ok = await self.__ioU.clearSession(uploadIds)
        if response.status_code != 200:
            logging.warning("error - could not delete session")
            return False
        return True

    async def clearKv(self):
        url = os.path.join(self.__hostAndPort, "file-v2", "clearKv")
        response = requests.post(url, data={}, headers=self.__headerD, timeout=None)