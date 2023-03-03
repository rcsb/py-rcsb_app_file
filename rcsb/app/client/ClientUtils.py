##
# File:    ClientUtils.py
# Author:  js
# Date:    22-Feb-2023
# Version: 0.001
#
# Updates: James Smith 2023
##
"""
Client utilities - wrapper of basic functionalities
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import os
import io
import logging
import tempfile
from copy import deepcopy
import math
import json
import requests
import typing
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.Definitions import Definitions
from fastapi.testclient import TestClient
from rcsb.app.file.main import app

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ClientUtils(object):
    def __init__(self, unit_test=False):
        configFilePath = os.environ.get("CONFIG_FILE", os.path.join("rcsb", "app", "config", "config.yml"))
        self.cP = ConfigProvider(configFilePath)
        self.cP.getConfig()
        self.baseUrl = self.cP.get("SERVER_HOST_AND_PORT")
        self.chunkSize = self.cP.get("CHUNK_SIZE")
        self.hashType = self.cP.get("HASH_TYPE")
        subject = self.cP.get("JWT_SUBJECT")
        self.headerD = {
            "Authorization": "Bearer " + JWTAuthToken(configFilePath).createToken({}, subject)
        }
        self.dP = Definitions()
        self.fileFormatExtensionD = self.dP.fileFormatExtD
        self.contentTypeInfoD = self.dP.contentTypeD
        self.repoTypeList = self.dP.repoTypeList
        self.milestoneList = self.dP.milestoneList
        self.__unit_test = unit_test

    # file parameter is complete file

    def upload(self, sourceFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, decompress, allowOverwrite, resumable):
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
            "hashDigest": fullTestHash,
            "allowOverwrite": allowOverwrite,
            "resumable": resumable,
        }
        url = os.path.join(self.baseUrl, "file-v2", "getUploadParameters")
        response = None
        if not self.__unit_test:
            response = requests.get(
                url,
                params=parameters,
                headers=self.headerD,
                timeout=None
            )
        else:
            with TestClient(app) as client:
                response = client.get(
                    url,
                    params=parameters,
                    headers=self.headerD,
                    timeout=None
                )
        # logger.info("status code %r", response.status_code)
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = result["chunkIndex"]
                uploadId = result["uploadId"]
        if not saveFilePath or not uploadId:
            logger.error("No file path or upload id were formed")
            return None
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
            "resumable": resumable,
            # save file parameters
            "filePath": saveFilePath,
            "decompress": decompress,
            "allowOverwrite": allowOverwrite,
        }
        offset = chunkIndex * self.chunkSize
        response = None
        tmp = io.BytesIO()
        with open(sourceFilePath, "rb") as fUpload:
            fUpload.seek(offset)
            url = os.path.join(self.baseUrl, "file-v2", "upload")
            for _ in range(chunkIndex, mD["expectedChunks"]):
                packetSize = min(
                    int(fileSize) - (int(mD["chunkIndex"]) * int(self.chunkSize)),
                    int(self.chunkSize),
                )
                tmp.truncate(packetSize)
                tmp.seek(0)
                tmp.write(fUpload.read(packetSize))
                tmp.seek(0)

                if not self.__unit_test:
                    response = requests.post(
                        url,
                        data=deepcopy(mD),
                        headers=self.headerD,
                        files={"chunk": tmp},
                        stream=True,
                        timeout=None,
                    )
                else:
                    with TestClient(app) as client:
                        response = client.post(
                            url,
                            data=deepcopy(mD),
                            headers=self.headerD,
                            files={"chunk": tmp},
                            timeout=None,
                        )

                if response.status_code != 200:
                    logger.error("Status code %r with text %r ...terminating", response.status_code, response.text)
                    break
                mD["chunkIndex"] += 1
        return response

    # file parameter is one chunk

    def getUploadParameters(self, sourceFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite, resumable):
        if not os.path.exists(sourceFilePath):
            logger.error("File does not exist: %r", sourceFilePath)
            return None
        # compress (externally), then hash, then upload
        # hash
        hD = CryptUtils().getFileHash(sourceFilePath, hashType=self.hashType)
        fullTestHash = hD["hashDigest"]
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
            "hashDigest": fullTestHash,
            "allowOverwrite": allowOverwrite,
            "resumable": resumable,
        }
        url = os.path.join(self.baseUrl, "file-v2", "getUploadParameters")

        if not self.__unit_test:
            response = requests.get(
                url,
                params=parameters,
                headers=self.headerD,
                timeout=None
            )
        else:
            with TestClient(app) as client:
                response = client.get(
                    url,
                    params=parameters,
                    headers=self.headerD,
                    timeout=None
                )

        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = result["chunkIndex"]
                uploadId = result["uploadId"]
        if not saveFilePath or not uploadId:
            logger.error("error - no file path or upload id were formed")
            return None
        # compute expected chunks
        fileSize = os.path.getsize(sourceFilePath)
        expectedChunks = 1
        if self.chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / self.chunkSize)
        return saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash

    # file parameter is one chunk

    def uploadChunk(self, sourceFilePath, saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash, decompress, allowOverwrite, resumable):
        if not os.path.exists(sourceFilePath):
            logger.error("File does not exist: %r", sourceFilePath)
            return None
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
            "resumable": resumable,
            # save file parameters
            "filePath": saveFilePath,
            "decompress": decompress,
            "allowOverwrite": allowOverwrite,
        }
        fileSize = os.path.getsize(sourceFilePath)
        offset = chunkIndex * self.chunkSize
        response = None
        tmp = io.BytesIO()
        with open(sourceFilePath, "rb") as toUpload:
            toUpload.seek(offset)
            packetSize = min(
                int(fileSize) - int(offset),
                int(self.chunkSize),
            )
            tmp.truncate(packetSize)
            tmp.seek(0)
            tmp.write(toUpload.read(packetSize))
            tmp.seek(0)
            url = os.path.join(self.baseUrl, "file-v2", "upload")

            if not self.__unit_test:
                response = requests.post(
                    url,
                    data=deepcopy(mD),
                    headers=self.headerD,
                    files={"chunk": tmp},
                    stream=True,
                    timeout=None,
                )
            else:
                with TestClient(app) as client:
                    response = client.post(
                    url,
                    data=deepcopy(mD),
                    headers=self.headerD,
                    files={"chunk": tmp},
                    timeout=None,
                    )

            if response.status_code != 200:
                logger.error("Terminating with status code %r and response text: %r", response.status_code, response.text)
        return response

    def download(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        hashType: str = "MD5",
        downloadFolder: typing.Optional[str] = None,
        allowOverwrite: bool = False,
        returnTempFile: bool = False
    ):
        convertedMilestone = None
        if not milestone or milestone.lower() == "none":
            milestone = ""
        if milestone and milestone.lower() != "none":
            convertedMilestone = f"-{milestone}"
        else:
            convertedMilestone = ""
        convertedContentFormat = self.fileFormatExtensionD[contentFormat]
        if not returnTempFile:
            if not os.path.exists(downloadFolder):
                logger.error("Download folder does not exist")
                return None
            fileName = f"{depId}_{contentType}{convertedMilestone}_P{partNumber}.{convertedContentFormat}.V{version}"
            downloadFilePath = os.path.join(downloadFolder, "download" + "_" + fileName)
            if os.path.exists(downloadFilePath):
                if not allowOverwrite:
                    logger.error("File already exists: %r", downloadFilePath)
                    return None
                os.remove(downloadFilePath)
        downloadDict = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version,
        }
        downloadSizeUrl = os.path.join(self.baseUrl, "file-v1", "downloadSize")

        if not self.__unit_test:
            fileSize = requests.get(downloadSizeUrl, params=downloadDict, headers=self.headerD, timeout=None).text
        else:
            with TestClient(app) as client:
                fileSize = client.get(downloadSizeUrl, params=downloadDict, headers=self.headerD, timeout=None).text

        if not fileSize.isnumeric():
            logger.error("no response for: %r", downloadFilePath)
            return None
        fileSize = int(fileSize)
        # chunks = math.ceil(fileSize / self.chunkSize)

        downloadUrlPrefix = os.path.join(self.baseUrl, "file-v1", "download")
        downloadUrl = (
            f"{downloadUrlPrefix}?repositoryType={repositoryType}&depId={depId}&contentType={contentType}&milestone={milestone}"
            f"&partNumber={partNumber}&contentFormat={contentFormat}&version={version}&hashType={hashType}"
        )
        resp = None

        if not self.__unit_test:
            if not returnTempFile:
                with requests.get(downloadUrl, headers=self.headerD, timeout=None, stream=True) as response:
                    with open(downloadFilePath, "ab") as ofh:
                        for chunk in response.iter_content(chunk_size=self.chunkSize):
                            if chunk:
                                ofh.write(chunk)
                    # responseCode = response.status_code
                    rspHashType = response.headers["rcsb_hash_type"]
                    rspHashDigest = response.headers["rcsb_hexdigest"]
                    thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
                    if not thD["hashDigest"] == rspHashDigest:
                        logger.error("Hash comparison failed")
                        return None
                    resp = response
            else:
                with requests.get(downloadUrl, headers=self.headerD, timeout=None, stream=True) as response:
                    ofh = tempfile.NamedTemporaryFile()
                    for chunk in response.iter_content(chunk_size=self.chunkSize):
                        if chunk:
                            ofh.write(chunk)
                    responseCode = response.status_code
                    rspHashType = response.headers["rcsb_hash_type"]
                    rspHashDigest = response.headers["rcsb_hexdigest"]
                    thD = CryptUtils().getFileHash(ofh.name, hashType=rspHashType)
                    if not thD["hashDigest"] == rspHashDigest:
                        logger.error("Hash comparison failed")
                        return None
                    resp = ofh
        else:
            resp = None
            if not returnTempFile:
                with TestClient(app) as client:
                    response = client.get(downloadUrl, headers=self.headerD, timeout=None)
                    with open(downloadFilePath, "ab") as ofh:
                        ofh.write(response.content)
                    responseCode = response.status_code
                    rspHashType = response.headers["rcsb_hash_type"]
                    rspHashDigest = response.headers["rcsb_hexdigest"]
                    thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
                    if not thD["hashDigest"] == rspHashDigest:
                        logger.error("Hash comparison failed")
                        return None
                    resp = responseCode
            else:
                with TestClient(app) as client:
                    response = client.get(downloadUrl, headers=self.headerD, timeout=None)
                    ofh = tempfile.NamedTemporaryFile()
                    ofh.write(response.content)
                    responseCode = response.status_code
                    rspHashType = response.headers["rcsb_hash_type"]
                    rspHashDigest = response.headers["rcsb_hexdigest"]
                    thD = CryptUtils().getFileHash(ofh.name, hashType=rspHashType)
                    if not thD["hashDigest"] == rspHashDigest:
                        logger.error("Hash comparison failed")
                        return None
                    resp = ofh
        return resp

    def listDir(self, repoType: str, depId: str) -> list:
        parameters = {
            "repositoryType": repoType,
            "depId": depId
        }
        if not depId or not repoType:
            logger.error("Missing values")
            return None
        url = os.path.join(self.baseUrl, "file-v1", "list-dir")
        responseCode = None
        dirList = None
        if not self.__unit_test:
            with requests.get(url, params=parameters, headers=self.headerD, timeout=None) as response:
                responseCode = response.status_code
                if responseCode == 200:
                    resp = response.text
                    if resp:
                        if not isinstance(resp, dict):
                            resp = json.loads(resp)
                        dirList = resp["dirList"]
        else:
            with TestClient(app) as client:
                response = client.get(url, params=parameters, headers=self.headerD, timeout=None)
                responseCode = response.status_code
                if responseCode == 200:
                    resp = response.text
                    if resp:
                        if not isinstance(resp, dict):
                            resp = json.loads(resp)
                        dirList = resp["dirList"]
        results = []
        if responseCode == 200:
            for fi in sorted(dirList):
                results.append(fi)
        return results
