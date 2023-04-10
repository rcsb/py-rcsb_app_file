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
import logging
import tempfile
import uuid
from copy import deepcopy
import math
import json
import requests
import typing
import pickle
from fastapi.testclient import TestClient
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.Definitions import Definitions
from rcsb.app.file.main import app
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.FileUtil import FileUtil


logger = logging.getLogger()
logger.setLevel(logging.INFO)


# require classes in same file to prevent circular reference
class FileAppObject(object):
    def __init__(
        self,
        repositoryType,
        depositId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
        hashType="MD5",
        unit_test=False,
    ):
        self.repositoryType = repositoryType
        self.depositId = depositId
        self.contentType = contentType
        self.milestone = milestone
        self.partNumber = partNumber
        self.contentFormat = contentFormat
        self.version = version
        self.hashType = hashType
        self.unit_test = unit_test
        self.clientContext = ClientContext(
            repositoryType,
            depositId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            hashType,
            unit_test,
        )


# dodge circular reference error from client utils by including class in same file
class ClientContext(object):
    def __init__(
        self,
        repositoryType,
        depositId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
        hashType="MD5",
        unit_test=False,
    ):
        self.repositoryType = repositoryType
        self.depositId = depositId
        self.contentType = contentType
        self.milestone = milestone
        self.partNumber = partNumber
        self.contentFormat = contentFormat
        self.version = version
        self.hashType = hashType
        self.unit_test = unit_test

    def __enter__(self):
        # download repository file
        # returns a local named temporary file
        downloadFolder = None
        allowOverwrite = True
        returnTempFile = True
        self.cU = ClientUtils(self.unit_test)
        self.file = self.cU.download(
            self.repositoryType,
            self.depositId,
            self.contentType,
            self.milestone,
            self.partNumber,
            self.contentFormat,
            self.version,
            self.hashType,
            downloadFolder,
            allowOverwrite,
            returnTempFile,
        )
        self.tempFilePath = self.file.name
        return self.file

    def __exit__(self, type, value, traceback):
        decompress = False
        allowOverwrite = True
        resumable = False
        # update repository file
        self.cU.upload(
            self.tempFilePath,
            self.repositoryType,
            self.depositId,
            self.contentType,
            self.milestone,
            self.partNumber,
            self.contentFormat,
            self.version,
            decompress,
            allowOverwrite,
            resumable
        )
        # delete local file
        self.file.close()


class ClientUtils(object):
    def __init__(self, unit_test=False):
        self.cP = ConfigProvider()
        self.cP.getConfig()
        self.baseUrl = self.cP.get("SERVER_HOST_AND_PORT")
        self.chunkSize = self.cP.get("CHUNK_SIZE")
        self.hashType = self.cP.get("HASH_TYPE")
        subject = self.cP.get("JWT_SUBJECT")
        self.headerD = {
            "Authorization": "Bearer "
            + JWTAuthToken().createToken({}, subject)
        }
        self.dP = Definitions()
        self.fileFormatExtensionD = self.dP.fileFormatExtD
        self.contentTypeInfoD = self.dP.contentTypeD
        self.repoTypeList = self.dP.repoTypeList
        self.milestoneList = self.dP.milestoneList
        self.__unit_test = unit_test

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
        resumable
    ):
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
            "hashDigest": fullTestHash,
            "resumable": resumable
        }
        url = os.path.join(self.baseUrl, "file-v2", "getUploadParameters")
        response = None
        if not self.__unit_test:
            response = requests.get(
                url, params=parameters, headers=self.headerD, timeout=None
            )
        else:
            with TestClient(app) as client:
                response = client.get(
                    url, params=parameters, headers=self.headerD, timeout=None
                )
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = int(result["chunkIndex"])
                uploadId = result["uploadId"]
                if chunkIndex > 0:
                    logger.info(f"detected upload with chunk index {chunkIndex}")
        if not saveFilePath:
            logger.error("No file path was formed")
            return None
        if not uploadId:
            logger.error("No upload id was formed")
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
            # save file parameters
            "filePath": saveFilePath,
            "decompress": decompress,
            "allowOverwrite": allowOverwrite,
            "resumable": resumable
        }
        offset = chunkIndex * self.chunkSize
        response = None
        with open(sourceFilePath, "rb") as fUpload:
            fUpload.seek(offset)
            url = os.path.join(self.baseUrl, "file-v2", "upload")
            for _ in range(chunkIndex, mD["expectedChunks"]):
                packetSize = min(
                    int(fileSize) - (int(mD["chunkIndex"]) * int(self.chunkSize)),
                    int(self.chunkSize),
                )
                # logger.info(f"packet size {packetSize} chunk {mD['chunkIndex']} expected {expectedChunks}")
                if not self.__unit_test:
                    response = requests.post(
                        url,
                        data=deepcopy(mD),
                        headers=self.headerD,
                        files={"chunk": fUpload.read(packetSize)},
                        stream=True,
                        timeout=None,
                    )
                else:
                    with TestClient(app) as client:
                        response = client.post(
                            url,
                            data=deepcopy(mD),
                            headers=self.headerD,
                            files={"chunk": fUpload.read(packetSize)},
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
        returnTempFile: bool = False,
        deleteTempFile: bool = True,
        chunkSize: typing.Optional[int] = None,
        chunkIndex: typing.Optional[int] = None
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

        downloadUrlPrefix = os.path.join(self.baseUrl, "file-v1", "download")
        suffix = ""
        if chunkSize and chunkIndex:
            suffix = f"&chunkSize={chunkSize}&chunkIndex={chunkIndex}"
        downloadUrl = (
            f"{downloadUrlPrefix}?repositoryType={repositoryType}&depId={depId}&contentType={contentType}&milestone={milestone}"
            f"&partNumber={partNumber}&contentFormat={contentFormat}&version={version}&hashType={hashType}{suffix}"
        )
        resp = None

        if not self.__unit_test:
            if not returnTempFile:
                with requests.get(
                    downloadUrl, headers=self.headerD, timeout=None, stream=True
                ) as response:
                    if response and response.status_code == 200:
                        with open(downloadFilePath, "ab") as ofh:
                            for chunk in response.iter_content(
                                chunk_size=self.chunkSize
                            ):
                                if chunk:
                                    ofh.write(chunk)
                        rspHashType = response.headers["rcsb_hash_type"]
                        rspHashDigest = response.headers["rcsb_hexdigest"]
                        thD = CryptUtils().getFileHash(
                            downloadFilePath, hashType=rspHashType
                        )
                        if not thD["hashDigest"] == rspHashDigest:
                            logger.error("Hash comparison failed")
                            return None
                        resp = response
            else:
                with requests.get(
                    downloadUrl, headers=self.headerD, timeout=None, stream=True
                ) as response:
                    if response and response.status_code == 200:
                        ofh = tempfile.NamedTemporaryFile(delete=deleteTempFile)
                        for chunk in response.iter_content(chunk_size=self.chunkSize):
                            if chunk:
                                ofh.write(chunk)
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
                    response = client.get(
                        downloadUrl, headers=self.headerD, timeout=None
                    )
                    if response and response.status_code == 200:
                        with open(downloadFilePath, "ab") as ofh:
                            ofh.write(response.content)
                        rspHashType = response.headers["rcsb_hash_type"]
                        rspHashDigest = response.headers["rcsb_hexdigest"]
                        thD = CryptUtils().getFileHash(
                            downloadFilePath, hashType=rspHashType
                        )
                        if not thD["hashDigest"] == rspHashDigest:
                            logger.error("Hash comparison failed")
                            return None
                        resp = response.status_code
            else:
                with TestClient(app) as client:
                    response = client.get(
                        downloadUrl, headers=self.headerD, timeout=None
                    )
                    if response and response.status_code == 200:
                        ofh = tempfile.NamedTemporaryFile(delete=deleteTempFile)
                        ofh.write(response.content)
                        ofh.seek(0)
                        rspHashType = response.headers["rcsb_hash_type"]
                        rspHashDigest = response.headers["rcsb_hexdigest"]
                        thD = CryptUtils().getFileHash(ofh.name, hashType=rspHashType)
                        if not thD["hashDigest"] == rspHashDigest:
                            logger.error("Hash comparison failed")
                            return None
                        resp = ofh
        return resp

    def listDir(self, repoType: str, depId: str) -> list:
        parameters = {"repositoryType": repoType, "depId": depId}
        if not depId or not repoType:
            logger.error("Missing values")
            return None
        url = os.path.join(self.baseUrl, "file-v1", "list-dir")
        responseCode = None
        dirList = None
        if not self.__unit_test:
            with requests.get(
                url, params=parameters, headers=self.headerD, timeout=None
            ) as response:
                responseCode = response.status_code
                if responseCode == 200:
                    resp = response.text
                    if resp:
                        if not isinstance(resp, dict):
                            resp = json.loads(resp)
                        dirList = resp["dirList"]
        else:
            with TestClient(app) as client:
                response = client.get(
                    url, params=parameters, headers=self.headerD, timeout=None
                )
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

    def getFileObject(
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
        fao = FileAppObject(
            repoType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            hashType,
            unit_test,
        )
        return fao

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
        pathU = PathUtils(self.cP)
        return pathU.getVersionedPath(
            repoType, depId, contentType, milestone, partNumber, contentFormat, version
        )

    def getFilePathLocal(
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
        downloadFolder = None
        allowOverwrite = True
        returnTempFile = True
        deleteTempFile = False
        file = self.download(
            repoType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            hashType,
            downloadFolder,
            allowOverwrite,
            returnTempFile,
            deleteTempFile,
        )
        return file.name

    def dirExist(self, repositoryType, depId):
        pathU = PathUtils(self.cP)
        dirPath = pathU.getDirPath(repositoryType, depId)
        fU = FileUtil()
        return fU.exists(dirPath)
