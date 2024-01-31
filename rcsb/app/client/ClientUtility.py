##
# File:    ClientUtility.py
# Author:  js
# Date:    22-Feb-2023
# Version: 1.0
#
# Updates: James Smith 2023
##

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import io
import os
import logging
from copy import deepcopy
import math
import json
import requests
import typing
from contextlib import contextmanager
from rcsb.app.file.IoUtility import IoUtility
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.Definitions import Definitions
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.UploadUtility import UploadUtility

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ClientUtility(object):
    """
    functions

    get-file-object, upload, get-upload-parameters, upload-chunk, download
    get-hash-digest, get-file-path-local, get-file-path-remote, dir-exists, list-dir
    copy-file, copy-dir, move-file, compress-dir, compress-dir-path, decompress-dir
    latest version, next version,
    file-size, file-exists

    """
    def __init__(self):
        self.cP = ConfigProvider()
        self.cP.getConfig()
        self.baseUrl = self.cP.get("SERVER_HOST_AND_PORT")
        self.chunkSize = self.cP.get("CHUNK_SIZE")
        self.compressionType = self.cP.get("COMPRESSION_TYPE")
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

    @contextmanager
    def getFileObject(
        self,
        repoType: str = None,
        depId: str = None,
        contentType: str = None,
        milestone: str = None,
        partNumber: int = None,
        contentFormat: str = None,
        version: str = None,
        sessionDir: str = None,
    ):
        # example: with ClientUtility().getFileObject(...) as file: file.write(...)
        if not os.path.exists(sessionDir):
            try:
                os.makedirs(sessionDir)
            except Exception:
                logger.exception(
                    "error - could not make session directory %s", sessionDir
                )
            return None
        download_file_path = os.path.join(
            sessionDir,
            PathProvider().getFileName(
                depId, contentType, milestone, partNumber, contentFormat, version
            ),
        )
        downloadFolder = sessionDir
        allowOverwrite = True  # overwrite pre-existing client file
        download_response = self.download(
            repositoryType=repoType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
            downloadFolder=downloadFolder,
            allowOverwrite=allowOverwrite,
            chunkSize=None,
            chunkIndex=None,
        )
        if download_response and download_response["status_code"] == 200:
            # return handle for reading and writing with pointer set to end of file by default
            # recommended - client should use file.seek prior to reading (file.seek(0)) or writing (file.seek(file.size))
            with open(download_file_path, "ab+") as file:
                try:
                    yield file
                finally:
                    file.close()
                    upload_response = self.upload(
                        download_file_path,
                        repoType,
                        depId,
                        contentType,
                        milestone,
                        partNumber,
                        contentFormat,
                        version,
                        decompress=False,
                        allowOverwrite=True,
                        resumable=False,
                    )
                    os.unlink(download_file_path)
                    if upload_response["status_code"] != 200:
                        logger.exception(
                            "upload error in get file object %d",
                            upload_response["status_code"],
                        )
        else:
            logger.exception("download error in get file object")
            yield None

    def open(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        sessionDir: str,
    ) -> typing.Optional[io.FileIO]:
        # obtain file pointer to a downloaded copy of a remote file
        # after modifying or reading file, must follow with close()
        # example: fp = ClientUtility().open(...), fp.write(...), ClientUtility().close(fp, ...)
        if not os.path.exists(sessionDir):
            try:
                os.makedirs(sessionDir)
            except Exception:
                logger.exception(
                    "error - could not make session directory %s", sessionDir
                )
            return None
        download_file_path = os.path.join(
            sessionDir,
            PathProvider().getFileName(
                depId, contentType, milestone, partNumber, contentFormat, version
            ),
        )
        downloadFolder = sessionDir
        allowOverwrite = True  # overwrite pre-existing client file
        download_response = self.download(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
            downloadFolder=downloadFolder,
            allowOverwrite=allowOverwrite,
            chunkSize=None,
            chunkIndex=None,
        )
        if download_response and download_response["status_code"] == 200:
            # return handle for reading and writing with pointer set to end of file by default
            # recommended - client should use file.seek prior to reading (file.seek(0)) or writing (file.seek(file.size))
            fp = open(download_file_path, "ab+")
            return fp
        logging.error("error %s", download_response["status_code"])
        return None

    def close(
        self,
        fp: io.FileIO,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        sessionDir: str,
        allowOverwrite: bool,
    ):
        # close file pointer obtained from open()
        # if allow overwrite, upload modified file
        # finally, delete temp file
        try:
            fp.close()
        except Exception:
            pass
        if not os.path.exists(sessionDir):
            logger.exception("error - session directory does not exist %s", sessionDir)
            return None
        download_file_path = os.path.join(
            sessionDir,
            PathProvider().getFileName(
                depId, contentType, milestone, partNumber, contentFormat, version
            ),
        )
        if not os.path.exists(download_file_path):
            logger.exception("error - file does not exist %s", download_file_path)
            return None
        if allowOverwrite:
            upload_response = self.upload(
                download_file_path,
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                decompress=False,
                allowOverwrite=allowOverwrite,
                resumable=False,
            )
            if upload_response["status_code"] >= 400:
                logger.exception(
                    "upload error %d",
                    upload_response["status_code"],
                )
        if os.path.exists(download_file_path):
            os.unlink(download_file_path)
        return {"status_code": upload_response["status_code"]}

    def makeDirs(self, repositoryType: str, depId: str):
        # makes repository type folder and dep id folder
        if repositoryType not in Definitions().repoTypeList:
            logger.exception("unrecognized repository type %s", repositoryType)
            return None
        url = os.path.join(self.baseUrl, "make-dirs")
        data = {"repositoryType": repositoryType, "depId": depId}
        response = requests.post(url, data=data, headers=self.headerD, timeout=None)
        if response.status_code >= 400:
            logger.warning("error - %s", response.status_code)
        return {"status_code": response.status_code}

    def makeDir(self, repositoryType: str, depId: str):
        # makes dep id folder if repository type folder already exists
        # if not, throws error
        if repositoryType not in Definitions().repoTypeList:
            logger.exception("unrecognized repository type %s", repositoryType)
            return None
        url = os.path.join(self.baseUrl, "make-dir")
        data = {"repositoryType": repositoryType, "depId": depId}
        response = requests.post(url, data=data, headers=self.headerD, timeout=None)
        if response.status_code >= 400:
            logger.warning("error - %s", response.status_code)
        return {"status_code": response.status_code}

    def join(
        self,
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    ):
        # returns non-absolute file path of a hypothetical deposition file on server
        url = os.path.join(self.baseUrl, "join")
        parameters = {
            "repositoryType": repositoryType,
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
            logger.info("error %s", response.status_code)
            return {"status_code": response.status_code}
        result = json.loads(response.text)
        if result["filePath"]:
            file_path = result["filePath"]
            return file_path
        logger.error("error - could not retrieve file path")
        return None

    # if file parameter is complete file

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
        decompress=False,
        fileExtension=None,
        allowOverwrite=False,
        resumable=False,
        extractChunk=True,
    ) -> dict:
        # validate input
        if not os.path.exists(sourceFilePath):
            logger.error("File does not exist: %r", sourceFilePath)
            return None
        fileExtension = (
            fileExtension if fileExtension else os.path.splitext(sourceFilePath)[-1]
        )
        # compress (externally), then hash and compute file size parameter, then upload
        # hash
        fullTestHash = IoUtility().getHashDigest(sourceFilePath, hashType=self.hashType)
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
            "resumable": resumable,
        }
        url = os.path.join(self.baseUrl, "getUploadParameters")
        response = requests.get(
            url, params=parameters, headers=self.headerD, timeout=None
        )

        if response.status_code == 200:
            logger.info("upload parameters - response %d", response.status_code)
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = int(result["chunkIndex"])
                uploadId = result["uploadId"]
                if chunkIndex > 0:
                    logger.info("detected upload with chunk index %s", chunkIndex)
        if not saveFilePath:
            logger.error("Error %d - no file path was formed", response.status_code)
            return {"status_code": response.status_code}
        if not uploadId:
            logger.error("Error %d - no upload id was formed", response.status_code)
            return {"status_code": response.status_code}

        # if file is already compressed, do not compress each chunk
        if decompress:
            extractChunk = False

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
            "fileSize": fileSize,
            "fileExtension": fileExtension,
            "decompress": decompress,
            "allowOverwrite": allowOverwrite,
            "resumable": resumable,
            "extractChunk": extractChunk,
        }
        offset = chunkIndex * self.chunkSize

        with open(sourceFilePath, "rb") as of:
            of.seek(offset)
            url = os.path.join(self.baseUrl, "upload")
            for _ in range(chunkIndex, mD["expectedChunks"]):
                offset = int(mD["chunkIndex"]) * int(self.chunkSize)
                packetSize = min(int(fileSize) - offset, int(self.chunkSize))
                chunk = of.read(packetSize)
                if extractChunk is None or extractChunk is True:
                    extractChunk = True
                    chunk = UploadUtility(self.cP).compressChunk(
                        chunk, self.compressionType
                    )
                    if not chunk:
                        logger.error("error - could not compress chunks")
                        return None
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
                    files={"chunk": chunk},
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

    # if file parameter is one chunk

    def getUploadParameters(
        self,
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
        allowOverwrite,
        resumable,
    ) -> dict:
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
            "resumable": resumable,
        }
        url = os.path.join(self.baseUrl, "getUploadParameters")
        response = requests.get(
            url, params=parameters, headers=self.headerD, timeout=None
        )
        if response.status_code == 200:
            logger.info("upload parameters - response %d", response.status_code)
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = int(result["chunkIndex"])
                uploadId = result["uploadId"]
                if chunkIndex > 0:
                    logger.info("detected upload with chunk index %s", chunkIndex)
        if not saveFilePath:
            logger.error("Error %d - no file path was formed", response.status_code)
            return {
                "status_code": response.status_code,
                "filePath": None,
                "chunkIndex": None,
                "uploadId": None,
            }
        if not uploadId:
            logger.error("Error %d - no upload id was formed", response.status_code)
            return {
                "status_code": response.status_code,
                "filePath": None,
                "chunkIndex": None,
                "uploadId": None,
            }
        return {
            "status_code": response.status_code,
            "filePath": saveFilePath,
            "chunkIndex": chunkIndex,
            "uploadId": uploadId,
        }

    def uploadChunk(
        self,
        sourceFilePath: str,
        # chunk parameters
        chunkSize: int,
        chunkIndex: int,
        expectedChunks: int,
        # upload file parameters
        uploadId: str,
        hashType: str,
        hashDigest: str,
        # save file parameters
        saveFilePath: str,
        fileSize: int,
        fileExtension: str = None,
        decompress: bool = False,
        allowOverwrite: bool = False,
        resumable: bool = False,
        extractChunk: bool = False,
    ) -> int:
        # validate input
        if not os.path.exists(sourceFilePath):
            logger.error("File does not exist: %r", sourceFilePath)
            return None
        fileExtension = (
            fileExtension if fileExtension else os.path.splitext(sourceFilePath)[-1]
        )
        offset = chunkIndex * chunkSize
        statusCode = 200
        with open(sourceFilePath, "rb") as of:
            of.seek(offset)
            url = os.path.join(self.baseUrl, "upload")
            packetSize = min(
                fileSize - offset,
                int(self.chunkSize),
            )
            chunk = of.read(packetSize)
            if not decompress:
                if extractChunk is None or extractChunk is True:
                    extractChunk = True
                    chunk = UploadUtility(self.cP).compressChunk(
                        chunk, self.compressionType
                    )
                    if not chunk:
                        logger.error("error compressing chunk")
                        return None
            logger.debug(
                "packet size %s chunk %s expected %s",
                packetSize,
                chunkIndex,
                expectedChunks,
            )
            mD = {
                # chunk parameters
                "chunkSize": chunkSize,
                "chunkIndex": chunkIndex,
                "expectedChunks": expectedChunks,
                # upload file parameters
                "uploadId": uploadId,
                "hashType": hashType,
                "hashDigest": hashDigest,
                # save file parameters
                "filePath": saveFilePath,
                "fileSize": fileSize,
                "fileExtension": fileExtension,
                "decompress": decompress,
                "allowOverwrite": allowOverwrite,
                "resumable": resumable,
                "extractChunk": extractChunk,
            }
            response = requests.post(
                url,
                data=mD,
                headers=self.headerD,
                files={"chunk": chunk},
                stream=True,
                timeout=None,
            )
            if response.status_code != 200:
                statusCode = response.status_code
                logger.error(
                    "Status code %r with text %r ...terminating",
                    response.status_code,
                    response.text,
                )
        return statusCode

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
        expectedChunks: typing.Optional[int] = None,
    ) -> dict:
        # validate input
        if not downloadFolder or not os.path.exists(downloadFolder):
            logger.error("Download folder does not exist %r", downloadFolder)
            return {"status_code": 404}
        chunks = False
        if (
            chunkSize is not None
            and chunkIndex is not None
            and expectedChunks is not None
        ):
            chunks = True

        # form paths
        fileName = PathProvider().getFileName(
            depId, contentType, milestone, partNumber, contentFormat, version
        )
        downloadFilePath = os.path.join(downloadFolder, fileName)

        # test file existence and overwrite settings
        if os.path.exists(downloadFilePath):
            if not chunks:
                if allowOverwrite:
                    os.unlink(downloadFilePath)
                    if os.path.exists(downloadFilePath):
                        logger.exception(
                            "error - could not remove %s", downloadFilePath
                        )
                else:
                    logger.error(
                        "error - overwrite not allowed on %s", downloadFilePath
                    )
                    return {"status_code": 403}
            # for chunks, partial file will exist until completion of download
            # however, file won't exist on first download
            elif chunkIndex == 0:
                if allowOverwrite:
                    os.unlink(downloadFilePath)
                    if os.path.exists(downloadFilePath):
                        logger.exception(
                            "error - could not remove %s", downloadFilePath
                        )
                else:
                    logger.error(
                        "error - overwrite not allowed on %s", downloadFilePath
                    )
                    return {"status_code": 403}

        # form query string
        hashType = self.hashType
        downloadUrlPrefix = os.path.join(self.baseUrl, "download")
        suffix = ""
        # optionally return one chunk
        if chunkSize is not None and chunkIndex is not None:
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
            # validate hash for non-chunked file or last chunk of file
            if not chunks or chunkIndex == expectedChunks - 1:
                # validate hash
                if (
                    "rcsb_hash_type" in response.headers
                    and "rcsb_hexdigest" in response.headers
                ):
                    rspHashType = response.headers["rcsb_hash_type"]
                    rspHashDigest = response.headers["rcsb_hexdigest"]
                    hashDigest = IoUtility().getHashDigest(
                        downloadFilePath, hashType=rspHashType
                    )
                    if not hashDigest == rspHashDigest:
                        logger.error("Hash comparison failed")
                        os.unlink(downloadFilePath)
                        return {"status_code": 400}

        return {
            "status_code": response.status_code,
            "file_path": downloadFilePath,
            "file_name": fileName,
        }

    def getHashDigest(
        self,
        repositoryType: str = None,
        depId: str = None,
        contentType: str = None,
        milestone: str = None,
        partNumber: int = None,
        contentFormat: str = None,
        version: str = None,
    ):
        query = f"repositoryType={repositoryType}&depId={depId}&contentType={contentType}&milestone={milestone}&partNumber={partNumber}&contentFormat={contentFormat}&version={version}"
        url = os.path.join(self.baseUrl, "get-hash?%s" % query)
        response = requests.get(url, headers=self.headerD, timeout=None)
        if response.status_code != 200:
            return {"status_code": response.status_code, "hashDigest": None}
        d = response.json()
        hashDigest = d["hashDigest"]
        return {"status_code": response.status_code, "hashDigest": hashDigest}

    def getFilePathRemote(
        self,
        repoType: str = None,
        depId: str = None,
        contentType: str = None,
        milestone: str = None,
        partNumber: int = None,
        contentFormat: str = None,
        version: str = None,
    ) -> dict:
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
            logger.info("error - requested file does not exist %s", parameters)
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
    ) -> dict:
        if not repoType or not depId or not contentType or not contentFormat:
            return {"status_code": 404, "content": None}
        path = PathProvider().getVersionedPath(
            repoType, depId, contentType, milestone, partNumber, contentFormat, version
        )
        # validate file exists on local machine
        if path and os.path.exists(path):
            # treat as web request for simplicity
            return {"status_code": 200, "filePath": path}
        logger.exception("error - path not found %s", path)
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

    def dirExists(self, repositoryType, depId) -> dict:
        url = os.path.join(
            self.baseUrl, f"dir-exists?repositoryType={repositoryType}&depId={depId}"
        )
        response = requests.get(url, headers=self.headerD, timeout=None)
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
    ) -> dict:
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
        response = requests.post(url, data=mD, headers=self.headerD, timeout=None)
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
    ) -> dict:
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
        response = requests.post(url, data=mD, headers=self.headerD, timeout=None)
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
    ) -> dict:
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
        response = requests.post(url, data=mD, headers=self.headerD, timeout=None)
        return {"status_code": response.status_code}

    def compressDir(self, repositoryType, depId) -> dict:
        mD = {"repositoryType": repositoryType, "depId": depId}
        url = os.path.join(self.baseUrl, "compress-dir")
        response = requests.post(url, data=mD, headers=self.headerD, timeout=None)
        return {"status_code": response.status_code}

    def compressDirPath(self, dirPath) -> dict:
        mD = {"dirPath": dirPath}
        url = os.path.join(self.baseUrl, "compress-dir-path")
        response = requests.post(url, data=mD, headers=self.headerD, timeout=None)
        return {"status_code": response.status_code}

    def decompressDir(self, repositoryType, depId) -> dict:
        mD = {"repositoryType": repositoryType, "depId": depId}
        url = os.path.join(self.baseUrl, "decompress-dir")
        response = requests.post(url, data=mD, headers=self.headerD, timeout=None)
        return {"status_code": response.status_code}

    def nextVersion(
        self, repositoryType, depId, contentType, milestone, partNumber, contentFormat
    ) -> dict:
        mD = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
        }
        url = os.path.join(self.baseUrl, "next-version")
        response = requests.get(url, params=mD, headers=self.headerD, timeout=None)
        if response.status_code == 200:
            result = response.json()
            return {"status_code": response.status_code, "version": result["version"]}
        else:
            return {"status_code": response.status_code, "version": None}

    def latestVersion(
        self, repositoryType, depId, contentType, milestone, partNumber, contentFormat
    ) -> dict:
        mD = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
        }
        url = os.path.join(self.baseUrl, "latest-version")
        response = requests.get(url, params=mD, headers=self.headerD, timeout=None)
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
    ) -> dict:
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
        response = requests.get(url, params=mD, headers=self.headerD, timeout=None)
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
    ) -> dict:
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
        response = requests.get(url, params=mD, headers=self.headerD, timeout=None)
        if response.status_code == 200:
            result = response.json()
            return {"status_code": response.status_code, "fileSize": result["fileSize"]}
        else:
            return {"status_code": response.status_code, "fileSize": None}
