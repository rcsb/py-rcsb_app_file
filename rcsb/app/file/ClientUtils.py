import asyncio
import copy
import sys
import os
import io
import gzip
from copy import deepcopy
import math
import requests
import json
import time
import typing
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider

"""
author James Smith 2023
"""

class ClientUtils(object):
    def __init__(self):
        self.base_url = "http://0.0.0.0:8000"
        self.chunkSize = 1024 * 1024 * 8
        self.hashType = "MD5"
        os.environ["CONFIG_FILE"] = os.path.join(".", "rcsb", "app", "config", "config.yml")
        configFilePath = os.environ.get("CONFIG_FILE")
        self.cP = ConfigProvider(configFilePath)
        self.cP.getConfig()
        subject = self.cP.get("JWT_SUBJECT")
        self.headerD = {
            "Authorization": "Bearer "
                             + JWTAuthToken(configFilePath).createToken({}, subject)
        }
        self.repoTypeList = self.cP.get("REPO_TYPE_LIST")
        self.milestoneList = self.cP.get("MILESTONE_LIST")
        self.fileFormatExtensionD = self.cP.get("FILE_FORMAT_EXTENSION")
        self.contentTypeInfoD = self.cP.get("CONTENT_TYPE")

    def upload(self, uploadMode, sourceFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite, copyMode):
        if not os.path.exists(sourceFilePath):
            sys.exit(f'error - file does not exist: {sourceFilePath}')
        # compress (externally), then hash, then upload
        # hash
        hD = CryptUtils().getFileHash(sourceFilePath, hashType=self.hashType)
        fullTestHash = hD["hashDigest"]
        fileSize = os.path.getsize(sourceFilePath)
        expectedChunks = 1
        if self.chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / self.chunkSize)
        chunkIndex = 0
        # upload as one file
        if uploadMode == 1:
            mD = {
                # upload file parameters
                "uploadId": None,
                "hashType": self.hashType,
                "hashDigest": fullTestHash,
                # save file parameters
                "repositoryType": repositoryType,
                "depId": depId,
                "contentType": contentType,
                "milestone": milestone,
                "partNumber": partNumber,
                "contentFormat": contentFormat,
                "version": version,
                "copyMode": copyMode,
                "allowOverwrite": allowOverwrite
            }
            response = None
            url = os.path.join(self.base_url, "file-v2", "upload")
            with open(sourceFilePath, "rb") as to_upload:
                response = requests.post(
                    url=url,
                    files={"uploadFile": to_upload},
                    stream=True,
                    data=deepcopy(mD),
                    headers=self.headerD,
                    timeout=None
                )
            if response and response.status_code != 200:
                sys.exit(
                    f"error - status code {response.status_code} {response.text}...terminating"
                )
            return response
        # upload chunks sequentially
        elif uploadMode == 2:
            mD = {
                # upload file parameters
                "uploadId": None,
                "hashType": self.hashType,
                "hashDigest": fullTestHash,
                # chunk parameters
                "chunkSize": self.chunkSize,
                "chunkIndex": chunkIndex,
                "expectedChunks": expectedChunks,
                # save file parameters
                "filePath": None,
                "copyMode": copyMode,
                "allowOverwrite": allowOverwrite
            }
            parameters = {"repositoryType": repositoryType,
                          "depId": depId,
                          "contentType": contentType,
                          "milestone": milestone,
                          "partNumber": str(partNumber),
                          "contentFormat": contentFormat,
                          "version": version,
                          "allowOverwrite": allowOverwrite
                          }
            url = os.path.join(self.base_url, "file-v2", "getSaveFilePath")
            response = requests.get(
                url,
                params=parameters,
                headers=self.headerD,
                timeout=None
            )
            if response.status_code == 200:
                result = json.loads(response.text)
                if result:
                    mD["filePath"] = result["path"]
            else:
                return [response]
            url = os.path.join(self.base_url, "file-v2", "getNewUploadId")
            response = requests.get(
                url,
                headers=self.headerD,
                timeout=None
            )
            if response.status_code == 200:
                result = json.loads(response.text)
                if result:
                    mD["uploadId"] = result["id"]
            else:
                return [response]
            # chunk file and upload
            offset = 0
            offsetIndex = 0
            responses = []
            tmp = io.BytesIO()
            with open(sourceFilePath, "rb") as to_upload:
                to_upload.seek(offset)
                url = os.path.join(self.base_url, "file-v2", "sequentialUpload")
                for x in range(offsetIndex, mD["expectedChunks"]):
                    packet_size = min(
                        int(fileSize) - (int(mD["chunkIndex"]) * int(mD["chunkSize"])),
                        int(mD["chunkSize"]),
                    )
                    tmp.truncate(packet_size)
                    tmp.seek(0)
                    tmp.write(to_upload.read(packet_size))
                    tmp.seek(0)
                    response = requests.post(
                        url,
                        data=deepcopy(mD),
                        headers=self.headerD,
                        files={"uploadFile": tmp},
                        stream=True,
                        timeout=None,
                    )
                    responses.append(response)
                    mD["chunkIndex"] += 1
            return responses
        # upload resumable sequential chunks
        elif uploadMode == 3:
            mD = {
                # upload file parameters
                "uploadId": None,
                "hashType": self.hashType,
                "hashDigest": fullTestHash,
                # chunk parameters
                "chunkSize": self.chunkSize,
                "chunkIndex": chunkIndex,
                "expectedChunks": expectedChunks,
                # save file parameters
                "repositoryType": repositoryType,
                "depId": depId,
                "contentType": contentType,
                "milestone": milestone,
                "partNumber": partNumber,
                "contentFormat": contentFormat,
                "version": version,
                "copyMode": copyMode,
                "allowOverwrite": allowOverwrite
            }
            uploadCount = 0
            # test for resumed upload
            parameters = {"repositoryType": mD["repositoryType"],
                          "depId": mD["depId"],
                          "contentType": mD["contentType"],
                          "milestone": mD["milestone"],
                          "partNumber": str(mD["partNumber"]),
                          "contentFormat": mD["contentFormat"],
                          "version": mD["version"],
                          "hashDigest": mD["hashDigest"]
                          }
            url = os.path.join(self.base_url, "file-v2", "uploadStatus")
            response = requests.get(
                url,
                params=parameters,
                headers=self.headerD,
                timeout=None
            )
            if response.status_code == 200:
                result = json.loads(response.text)
                if result:
                    result = eval(result)
                    uploadCount = result["uploadCount"]
            responses = []
            for index in range(uploadCount, expectedChunks):
                offset = index * self.chunkSize
                mD["chunkIndex"] = index
                tmp = io.BytesIO()
                with open(sourceFilePath, "rb") as to_upload:
                    to_upload.seek(offset)
                    packet_size = min(
                        int(fileSize) - (int(mD["chunkIndex"]) * int(mD["chunkSize"])),
                        int(mD["chunkSize"]),
                    )
                    tmp.truncate(packet_size)
                    tmp.seek(0)
                    tmp.write(to_upload.read(packet_size))
                    tmp.seek(0)
                    url = os.path.join(self.base_url, "file-v2", "resumableUpload")
                    response = requests.post(
                        url,
                        data=mD,
                        headers=self.headerD,
                        files={"uploadFile": tmp},
                        stream=True,
                        timeout=None,
                    )
                    responses.append(response)
            return responses

    def download(self, repositoryType: str, depId: str, contentType: str, milestone: str, partNumber: int, contentFormat: str, version: str, hashType: str, downloadFolder: str, allowOverwrite: bool):
        if not os.path.exists(downloadFolder):
            print('error - download folder does not exist')
            return None
        convertedMilestone = None
        if milestone and milestone.lower() != 'none':
            convertedMilestone = f'-{milestone}'
        else:
            convertedMilestone = ""
        convertedContentFormat = self.fileFormatExtensionD[contentFormat]
        fileName = f'{depId}_{contentType}{convertedMilestone}_P{partNumber}.{convertedContentFormat}.V{version}'
        downloadFilePath = os.path.join(downloadFolder, "download" + "_" + fileName)
        if not os.path.exists(downloadFolder):
            print(f'error - folder does not exist: {downloadFolder}')
            return None
        if os.path.exists(downloadFilePath):
            if not allowOverwrite:
                print(f'error - file already exists: {downloadFilePath}')
                return None
            os.remove(downloadFilePath)
        if milestone.lower() == "none":
            milestone = ""
        downloadDict = {
            "repositoryType": repositoryType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version
        }
        url = os.path.join(self.base_url, "file-v1", "downloadSize")
        fileSize = requests.get(url, params=downloadDict, headers=self.headerD, timeout=None).text
        if not fileSize.isnumeric():
            print(f'error - no response for {downloadFilePath}')
            return None
        fileSize = int(fileSize)
        chunks = math.ceil(fileSize / self.chunkSize)
        url = os.path.join(self.base_url, "file-v1", "download")
        url = f'{url}?repositoryType={repositoryType}&depId={depId}&contentType={contentType}&milestone={milestone}&partNumber={partNumber}&contentFormat={contentFormat}&version={version}&hashType={hashType}'
        resp = None
        with requests.get(url, headers=self.headerD, timeout=None, stream=True) as response:
            with open(downloadFilePath, "ab") as ofh:
                for chunk in response.iter_content(chunk_size=self.chunkSize):
                    if chunk:
                        ofh.write(chunk)
            responseCode = response.status_code
            rspHashType = response.headers["rcsb_hash_type"]
            rspHashDigest = response.headers["rcsb_hexdigest"]
            thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
            if not thD["hashDigest"] == rspHashDigest:
                print('error - hash comparison failed')
                return None
            resp = response
        return resp

    def listDir(self, repoType: str, depId: str) -> list:
        parameters = {
            "repositoryType": repoType,
            "depId": depId
        }
        if not depId or not repoType:
            print('error - missing values')
            return None
        url = os.path.join(self.base_url, "file-v1", "list-dir")
        responseCode = None
        dirList = None
        with requests.get(url, params=parameters, headers=self.headerD, timeout=None) as response:
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


