import asyncio
import copy
import sys
import os
import io
import gzip
from copy import deepcopy
import math
import requests
from http.client import HTTPResponse
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

    # file parameter is complete file

    def upload(self, sourceFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, decompress, allowOverwrite, resumable):
        if not os.path.exists(sourceFilePath):
            print(f'error - file does not exist: {sourceFilePath}')
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
        parameters = {"repositoryType": repositoryType,
                      "depId": depId,
                      "contentType": contentType,
                      "milestone": milestone,
                      "partNumber": partNumber,
                      "contentFormat": contentFormat,
                      "version": version,
                      "hashDigest": fullTestHash,
                      "allowOverwrite": allowOverwrite,
                      "resumable": resumable
                      }
        url = os.path.join(self.base_url, "file-v2", "getUploadParameters")
        response = requests.get(
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
            print('error - no file path or upload id were formed')
            return response
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
            "allowOverwrite": allowOverwrite
        }
        offset = chunkIndex * self.chunkSize
        response = None
        tmp = io.BytesIO()
        with open(sourceFilePath, "rb") as to_upload:
            to_upload.seek(offset)
            url = os.path.join(self.base_url, "file-v2", "upload")
            for x in range(chunkIndex, mD["expectedChunks"]):
                packet_size = min(
                    int(fileSize) - (int(mD["chunkIndex"]) * int(self.chunkSize)),
                    int(self.chunkSize),
                )
                tmp.truncate(packet_size)
                tmp.seek(0)
                tmp.write(to_upload.read(packet_size))
                tmp.seek(0)
                response = requests.post(
                    url,
                    data=deepcopy(mD),
                    headers=self.headerD,
                    files={"chunk": tmp},
                    stream=True,
                    timeout=None,
                )
                if response.status_code != 200:
                    print(f"error - status code {response.status_code} {response.text}...terminating")
                    break
                mD["chunkIndex"] += 1
        return response

    # file parameter is one chunk

    def getUploadParameters(self, sourceFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite, resumable):
        if not os.path.exists(sourceFilePath):
            print(f'error - file does not exist: {sourceFilePath}')
            return None
        # compress (externally), then hash, then upload
        # hash
        hD = CryptUtils().getFileHash(sourceFilePath, hashType=self.hashType)
        fullTestHash = hD["hashDigest"]
        # get upload parameters
        saveFilePath = None
        chunkIndex = 0
        uploadId = None
        parameters = {"repositoryType": repositoryType,
                      "depId": depId,
                      "contentType": contentType,
                      "milestone": milestone,
                      "partNumber": partNumber,
                      "contentFormat": contentFormat,
                      "version": version,
                      "hashDigest": fullTestHash,
                      "allowOverwrite": allowOverwrite,
                      "resumable": resumable
                      }
        url = os.path.join(self.base_url, "file-v2", "getUploadParameters")
        response = requests.get(
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
            print('error - no file path or upload id were formed')
            return response
        # compute expected chunks
        fileSize = os.path.getsize(sourceFilePath)
        expectedChunks = 1
        if self.chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / self.chunkSize)
        return saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash

    # file parameter is one chunk

    def uploadChunk(self, sourceFilePath, saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash, decompress, allowOverwrite, resumable):
        if not os.path.exists(sourceFilePath):
            print(f'error - file does not exist: {sourceFilePath}')
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
            "allowOverwrite": allowOverwrite
        }
        fileSize = os.path.getsize(sourceFilePath)
        offset = chunkIndex * self.chunkSize
        response = None
        tmp = io.BytesIO()
        with open(sourceFilePath, "rb") as to_upload:
            to_upload.seek(offset)
            packet_size = min(
                int(fileSize) - int(offset),
                int(self.chunkSize),
            )
            tmp.truncate(packet_size)
            tmp.seek(0)
            tmp.write(to_upload.read(packet_size))
            tmp.seek(0)
            url = os.path.join(self.base_url, "file-v2", "upload")
            response = requests.post(
                url,
                data=deepcopy(mD),
                headers=self.headerD,
                files={"chunk": tmp},
                stream=True,
                timeout=None,
            )
            if response.status_code != 200:
                print(f"error - status code {response.status_code} {response.text}...terminating")
        return response

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


