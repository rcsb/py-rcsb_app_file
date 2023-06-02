##
# File:    ClientUtils.py
# Author:  js
# Date:    22-Feb-2023
# Version: 1.0
#
# Updates: James Smith 2023
##

"""
functions

upload, download
get-file-path-local, get-file-path-remote, dir-exists, list-dir
copy-file, copy-dir, move-file, compress-dir, compress-dir-path, decompress-dir
latest version, next version,
file-size, file-exists

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import os
import logging
from copy import deepcopy
import math
import json
import requests
import typing
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.Definitions import Definitions
from rcsb.app.file.PathProvider import PathProvider

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ClientUtils(object):
    def __init__(self):
        self.cP = ConfigProvider()
        self.cP.getConfig()
        self.baseUrl = self.cP.get("SERVER_HOST_AND_PORT")
        self.chunkSize = self.cP.get("CHUNK_SIZE")
        self.hashType = self.cP.get("HASH_TYPE")
        subject = self.cP.get("JWT_SUBJECT")
        self.headerD = {
            "Authorization": "Bearer " + JWTAuthToken().createToken({}, subject)
        }
        self.dP = Definitions()
        self.fileFormatExtensionD = self.dP.fileFormatExtD
        self.contentTypeInfoD = self.dP.contentTypeD
        self.repoTypeList = self.dP.repoTypeList
        self.milestoneList = self.dP.milestoneList

    # file parameter is complete file

    def upload(
        self,
        sourceFilePath,
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
        decompress,
        allowOverwrite,
        resumable,
    ) -> dict:
        # validate input
        if not os.path.exists(sourceFilePath):
            logger.error("File does not exist: %r", sourceFilePath)
            return None
        # compress (externally), then hash, then upload
        # hash
        hD = CryptUtils().getFileHash(sourceFilePath, hashType=self.hashType)
        fullTestHash = hD["hashDigest"]
        # compute expected chunks
        fileSize = os.path.getsize(sourceFilePath)
        expectedChunks = 1
        if self.chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / self.chunkSize)
        # get upload parameters
        saveFilePath = None
        chunkIndex = 0
        uploadId = None
        parameters = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version,
            "allowOverwrite": allowOverwrite,
            # "hashDigest": fullTestHash,
            "resumable": resumable,
        }
        url = os.path.join(self.baseUrl, "getUploadParameters")
        response = requests.get(
            url, params=parameters, headers=self.headerD, timeout=None
        )

        if response.status_code == 200:
            logger.info(f"upload parameters - response {response.status_code}")
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = int(result["chunkIndex"])
                uploadId = result["uploadId"]
                if chunkIndex > 0:
                    logger.info(f"detected upload with chunk index {chunkIndex}")
        if not saveFilePath:
            logger.error(f"Error {response.status_code} - no file path was formed")
            return {"status_code": response.status_code}
        if not uploadId:
            logger.error(f"Error {response.status_code} - no upload id was formed")
            return {"status_code": response.status_code}

        # chunk file and upload
        mD = {
            # chunk parameters
            "chunkSize": self.chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # upload file parameters
            "uploadId": uploadId,
            "hashType": self.hashType,
            "hashDigest": fullTestHash,
            # save file parameters
            "filePath": saveFilePath,
            "decompress": decompress,
            "allowOverwrite": allowOverwrite,
            "resumable": resumable,
        }
        offset = chunkIndex * self.chunkSize

        with open(sourceFilePath, "rb") as of:
            of.seek(offset)
            url = os.path.join(self.baseUrl, "upload")
            for _ in range(chunkIndex, mD["expectedChunks"]):
                packetSize = min(
                    int(fileSize) - (int(mD["chunkIndex"]) * int(self.chunkSize)),
                    int(self.chunkSize),
                )
                logger.debug(
                    "packet size %s chunk %s expected %s",
                    packetSize,
                    mD["chunkIndex"],
                    expectedChunks,
                )

                response = requests.post(
                    url,
                    data=deepcopy(mD),
                    headers=self.headerD,
                    files={"chunk": of.read(packetSize)},
                    stream=True,
                    timeout=None,
                )

                if response.status_code != 200:
                    logger.error(
                        "Status code %r with text %r ...terminating",
                        response.status_code,
                        response.text,
                    )
                    break
                mD["chunkIndex"] += 1

        return {"status_code": response.status_code}

    def download(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: int,
        downloadFolder: typing.Optional[str] = None,
        allowOverwrite: bool = False,
        chunkSize: typing.Optional[int] = None,
        chunkIndex: typing.Optional[int] = None,
    ) -> dict:
        # form paths
        fileName = PathProvider().getFileName(
            depId, contentType, milestone, partNumber, contentFormat, version
        )
        downloadFilePath = os.path.join(downloadFolder, fileName)

        # validate input
        if not os.path.exists(downloadFolder):
            logger.error("Download folder does not exist")
            return None
        if os.path.exists(downloadFilePath):
            if not allowOverwrite:
                logger.error("File already exists: %r", downloadFilePath)
                return {"status_code": 403}
            os.remove(downloadFilePath)

        # form query string
        hashType = self.hashType
        downloadUrlPrefix = os.path.join(self.baseUrl, "download")
        suffix = ""
        # optionally return one chunk
        if chunkSize and chunkIndex:
            suffix = f"&chunkSize={chunkSize}&chunkIndex={chunkIndex}"
        downloadUrl = (
            f"{downloadUrlPrefix}?repositoryType={repositoryType}&depId={depId}&contentType={contentType}&milestone={milestone}"
            f"&partNumber={partNumber}&contentFormat={contentFormat}&version={version}&hashType={hashType}{suffix}"
        )

        # download file to folder, return http response
        response = requests.get(
            downloadUrl, headers=self.headerD, timeout=None, stream=True
        )
        if response and response.status_code == 200:
            # write to file
            with open(downloadFilePath, "ab") as ofh:
                for chunk in response.iter_content(chunk_size=self.chunkSize):
                    if chunk:
                        ofh.write(chunk)
            # validate hash
            rspHashType = response.headers["rcsb_hash_type"]
            rspHashDigest = response.headers["rcsb_hexdigest"]
            thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
            if not thD["hashDigest"] == rspHashDigest:
                logger.error("Hash comparison failed")
                return None

        return {"status_code": response.status_code}

    def getFilePathRemote(
        self,
        repoType: str = None,
        depId: str = None,
        contentType: str = None,
        milestone: str = None,
        partNumber: int = None,
        contentFormat: str = None,
        version: str = None,
        hashType: str = "MD5",
        unit_test: bool = False,
        wfInstanceId: str = None,
        sessionDir: str = None,
    ):
        # validate file exists
        url = os.path.join(self.baseUrl, "file-exists")
        parameters = {
            "repositoryType": repoType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version,
        }
        response = requests.get(
            url, params=parameters, headers=self.headerD, timeout=None
        )
        if response.status_code != 200:
            logger.info(f"error - requested file does not exist {parameters}")
            return {"status_code": response.status_code, "content": None}
        # return absolute file path on server
        url = os.path.join(self.baseUrl, "file-path")
        response = requests.get(
            url, params=parameters, headers=self.headerD, timeout=None
        )
        if response.status_code == 200:
            result = response.json()
            return {"status_code": response.status_code, "filePath": result["filePath"]}
        else:
            return {"status_code": response.status_code, "filePath": None}

    def getFilePathLocal(
        self,
        repoType: str = None,
        depId: str = None,
        contentType: str = None,
        milestone: str = "",
        partNumber: int = 1,
        contentFormat: str = None,
        version: str = "next",
        hashType: str = "MD5",
        wfInstanceId: str = None,
        sessionDir: str = None,
    ):
        if not repoType or not depId or not contentType or not contentFormat:
            return {"status_code": 404, "content": None}
        path = PathProvider().getVersionedPath(
            repoType, depId, contentType, milestone, partNumber, contentFormat, version
        )
        # validate file exists on local machine
        if path and os.path.exists(path):
            # treat as web request for simplicity
            return {"status_code": 200, "filePath": path}
        logger.exception("error - path not found %s" % path)
        return {"status_code": 404, "filePath": None}

    def listDir(self, repoType: str, depId: str) -> dict:
        if not depId or not repoType:
            logger.error("Missing values")
            return None
        url = os.path.join(self.baseUrl, "list-dir")
        parameters = {"repositoryType": repoType, "depId": depId}
        response = requests.get(
            url, params=parameters, headers=self.headerD, timeout=None
        )
        if response and response.status_code == 200:
            dirList = []
            resp = response.json()
            if resp:
                results = resp["dirList"]
                for fi in sorted(results):
                    dirList.append(fi)
            return {"status_code": response.status_code, "dirList": dirList}
        else:
            return {"status_code": response.status_code, "dirList": None}

    def dirExists(self, repositoryType, depId):
        url = os.path.join(
            self.baseUrl, f"dir-exists?repositoryType={repositoryType}&depId={depId}"
        )
        response = requests.get(url, headers=self.headerD)
        return {"status_code": response.status_code}

    def copyFile(
        self,
        repositoryTypeSource,
        depIdSource,
        contentTypeSource,
        milestoneSource,
        partNumberSource,
        contentFormatSource,
        versionSource,
        #
        repositoryTypeTarget,
        depIdTarget,
        contentTypeTarget,
        milestoneTarget,
        partNumberTarget,
        contentFormatTarget,
        versionTarget,
        #
        overwrite,
    ):
        mD = {
            "repositoryTypeSource": repositoryTypeSource,
            "depIdSource": depIdSource,
            "contentTypeSource": contentTypeSource,
            "milestoneSource": milestoneSource,
            "partNumberSource": partNumberSource,
            "contentFormatSource": contentFormatSource,
            "versionSource": versionSource,
            #
            "repositoryTypeTarget": repositoryTypeTarget,
            "depIdTarget": depIdTarget,
            "contentTypeTarget": contentTypeTarget,
            "milestoneTarget": milestoneTarget,
            "partNumberTarget": partNumberTarget,
            "contentFormatTarget": contentFormatTarget,
            "versionTarget": versionTarget,
            #
            "overwrite": overwrite,
        }
        url = os.path.join(self.baseUrl, "copy-file")
        response = requests.post(url, data=mD, headers=self.headerD)
        return {"status_code": response.status_code}

    def copyDir(
        self,
        repositoryTypeSource,
        depIdSource,
        #
        repositoryTypeTarget,
        depIdTarget,
        #
        overwrite,
    ):
        mD = {
            "repositoryTypeSource": repositoryTypeSource,
            "depIdSource": depIdSource,
            #
            "repositoryTypeTarget": repositoryTypeTarget,
            "depIdTarget": depIdTarget,
            #
            "overwrite": overwrite,
        }
        url = os.path.join(self.baseUrl, "copy-dir")
        response = requests.post(url, data=mD, headers=self.headerD)
        return {"status_code": response.status_code}

    def moveFile(
        self,
        repositoryTypeSource,
        depIdSource,
        contentTypeSource,
        milestoneSource,
        partNumberSource,
        contentFormatSource,
        versionSource,
        #
        repositoryTypeTarget,
        depIdTarget,
        contentTypeTarget,
        milestoneTarget,
        partNumberTarget,
        contentFormatTarget,
        versionTarget,
        #
        overwrite,
    ):
        mD = {
            "repositoryTypeSource": repositoryTypeSource,
            "depIdSource": depIdSource,
            "contentTypeSource": contentTypeSource,
            "milestoneSource": milestoneSource,
            "partNumberSource": partNumberSource,
            "contentFormatSource": contentFormatSource,
            "versionSource": versionSource,
            #
            "repositoryTypeTarget": repositoryTypeTarget,
            "depIdTarget": depIdTarget,
            "contentTypeTarget": contentTypeTarget,
            "milestoneTarget": milestoneTarget,
            "partNumberTarget": partNumberTarget,
            "contentFormatTarget": contentFormatTarget,
            "versionTarget": versionTarget,
            #
            "overwrite": overwrite,
        }
        url = os.path.join(self.baseUrl, "move-file")
        response = requests.post(url, data=mD, headers=self.headerD)
        return {"status_code": response.status_code}

    def compressDir(self, repositoryType, depId):
        mD = {"repositoryType": repositoryType, "depId": depId}
        url = os.path.join(self.baseUrl, "compress-dir")
        response = requests.post(url, data=mD, headers=self.headerD)
        return {"status_code": response.status_code}

    def compressDirPath(self, dirPath):
        mD = {"dirPath": dirPath}
        url = os.path.join(self.baseUrl, "compress-dir-path")
        response = requests.post(url, data=mD, headers=self.headerD)
        return {"status_code": response.status_code}

    def decompressDir(self, repositoryType, depId):
        mD = {"repositoryType": repositoryType, "depId": depId}
        url = os.path.join(self.baseUrl, "decompress-dir")
        response = requests.post(url, data=mD, headers=self.headerD)
        return {"status_code": response.status_code}

    def nextVersion(
        self, repositoryType, depId, contentType, milestone, partNumber, contentFormat
    ):
        mD = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
        }
        url = os.path.join(self.baseUrl, "next-version")
        response = requests.get(url, params=mD, headers=self.headerD)
        if response.status_code == 200:
            result = response.json()
            return {"status_code": response.status_code, "version": result["version"]}
        else:
            return {"status_code": response.status_code, "version": None}

    def latestVersion(
        self, repositoryType, depId, contentType, milestone, partNumber, contentFormat
    ):
        mD = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
        }
        url = os.path.join(self.baseUrl, "latest-version")
        response = requests.get(url, params=mD, headers=self.headerD)
        if response.status_code == 200:
            result = response.json()
            return {"status_code": response.status_code, "version": result["version"]}
        else:
            return {"status_code": response.status_code, "version": None}

    def fileExists(
        self,
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    ):
        mD = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version,
        }
        url = os.path.join(self.baseUrl, "file-exists")
        response = requests.get(url, params=mD, headers=self.headerD)
        return {"status_code": response.status_code}

    def fileSize(
        self,
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    ):
        mD = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version,
        }
        url = os.path.join(self.baseUrl, "file-size")
        response = requests.get(url, params=mD, headers=self.headerD)
        if response.status_code == 200:
            result = response.json()
            return {"status_code": response.status_code, "fileSize": result["fileSize"]}
        else:
            return {"status_code": response.status_code, "fileSize": None}
