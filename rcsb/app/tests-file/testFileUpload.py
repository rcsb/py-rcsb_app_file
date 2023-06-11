# file - testFileUpload.py
# author - James Smith 2023

import json
import re
import sys
import unittest
import os
import shutil
import logging
import time
from copy import deepcopy
import math
from fastapi.testclient import TestClient
from rcsb.app.file.UploadUtility import UploadUtility
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider

logging.basicConfig(level=logging.INFO)

class UploadTest(unittest.TestCase):
    def setUp(self):
        self.__cP = ConfigProvider()
        self.__baseUrl = self.__cP.get("SERVER_HOST_AND_PORT")
        self.__configFilePath = self.__cP.getConfigFilePath()
        subject = self.__cP.get("JWT_SUBJECT")
        self.__headerD = {
            "Authorization": "Bearer "
            + JWTAuthToken().createToken({}, subject)
        }
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__hashType = self.__cP.get("HASH_TYPE")
        self.__dataPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__repositoryType = "unit-test"
        self.__depId = "D_1000000001"
        self.__contentType = "model"
        self.__milestone = "upload"
        self.__convertedMilestone = ""
        if self.__milestone is not None and self.__milestone != "":
            self.__convertedMilestone = "-" + self.__milestone
        self.__partNumber = 1
        self.__contentFormat = "pdbx"
        self.__convertedContentFormat = None
        if self.__contentFormat == "pdbx":
            self.__convertedContentFormat = "cif"
        else:
            logging.error("error - please refer to upload functions for other examples of content format conversion")
            sys.exit()
        self.__version = "1"
        if not re.match(r"^\d+$", self.__version):
            logging.error("error - have not encoded logic for non-numeric version numbers")
            sys.exit()
        self.__repoFileName = f"{self.__depId}_{self.__contentType}{self.__convertedMilestone}_P{self.__partNumber}.{self.__contentFormat}.V{self.__version}"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        logging.info("self.__dataPath %s", self.__dataPath)

        self.__repositoryFile = os.path.join(self.__unitTestFolder, self.__depId, self.__repoFileName)
        if os.path.exists(self.__repositoryFile):
            os.unlink(self.__repositoryFile)
        os.makedirs(os.path.dirname(self.__repositoryFile), mode=0o757, exist_ok=True)

        self.__dataFile = os.path.join(self.__dataPath, "testFile.dat")
        if not os.path.exists(self.__dataFile):
            os.makedirs(os.path.dirname(self.__dataFile), mode=0o757, exist_ok=True)
            nB = self.__chunkSize * 2
            with open(self.__dataFile, "wb") as out:
                out.write(os.urandom(nB))

        self.__startTime = time.time()
        logging.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if os.path.exists(self.__repositoryFile):
            os.unlink(self.__repositoryFile)
        if os.path.exists(self.__dataFile):
            os.unlink(self.__dataFile)
        # warning - do not delete the data/repository folder for production, just the unit-test folder within that folder
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        endTime = time.time()
        logging.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleUpload(self):
        if not os.path.exists(self.__dataFile):
            logging.error("File does not exist: %r", self.__dataFile)
            return None
        # compress (externally), then hash, then upload
        # hash
        hD = CryptUtils().getFileHash(self.__dataFile, hashType=self.__hashType)
        fullTestHash = hD["hashDigest"]
        # compute expected chunks
        fileSize = os.path.getsize(self.__dataFile)
        expectedChunks = 1
        if self.__chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / self.__chunkSize)
        # get upload parameters
        saveFilePath = None
        chunkIndex = 0
        uploadId = None
        parameters = {
            "repositoryType": self.__repositoryType,
            "depId": self.__depId,
            "contentType": self.__contentType,
            "milestone": self.__milestone,
            "partNumber": self.__partNumber,
            "contentFormat": self.__contentFormat,
            "version": self.__version,
            "allowOverwrite": False,
            "resumable": False
        }
        url = os.path.join(self.__baseUrl, "getUploadParameters")
        response = None
        with TestClient(app) as client:
            response = client.get(
                url, params=parameters, headers=self.__headerD, timeout=None
            )
        if not response or not response.status_code:
            logging.error("error - no response")
            sys.exit()
        self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = int(result["chunkIndex"])
                uploadId = result["uploadId"]
                if chunkIndex > 0:
                    logging.info(f"detected upload with chunk index {chunkIndex}")
        self.assertTrue(saveFilePath is not None)
        self.assertTrue(uploadId is not None)
        self.assertTrue(chunkIndex == 0)
        if not saveFilePath:
            logging.error("No file path was formed")
            return None
        if not uploadId:
            logging.error("No upload id was formed")
            return None

        # chunk file and upload
        mD = {
            # chunk parameters
            "chunkSize": self.__chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # upload file parameters
            "uploadId": uploadId,
            "hashType": self.__hashType,
            "hashDigest": fullTestHash,
            # save file parameters
            "filePath": saveFilePath,
            "decompress": False,
            "allowOverwrite": False,
            "resumable": False
        }
        offset = chunkIndex * self.__chunkSize
        response = None
        with open(self.__dataFile, "rb") as of:
            of.seek(offset)
            url = os.path.join(self.__baseUrl, "upload")
            for _ in range(chunkIndex, mD["expectedChunks"]):
                packetSize = min(
                    int(fileSize) - (int(mD["chunkIndex"]) * int(self.__chunkSize)),
                    int(self.__chunkSize),
                )
                logging.debug("packet size %s chunk %s expected %s", packetSize, mD['chunkIndex'], expectedChunks)
                with TestClient(app) as client:
                    response = client.post(
                        url,
                        data=deepcopy(mD),
                        headers=self.__headerD,
                        files={"chunk": of.read(packetSize)},
                        timeout=None,
                    )
                if not response or not response.status_code:
                    logging.error(
                        "Status code %r with text %r ...terminating",
                        response.status_code,
                        response.text,
                    )
                    break
                self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
                mD["chunkIndex"] += 1
        return response

    def testSimpleUpdate(self):
        if not os.path.exists(self.__dataFile):
            logging.error("File does not exist: %r", self.__dataFile)
            return None
        # compress (externally), then hash, then upload
        # hash
        hD = CryptUtils().getFileHash(self.__dataFile, hashType=self.__hashType)
        fullTestHash = hD["hashDigest"]
        # compute expected chunks
        fileSize = os.path.getsize(self.__dataFile)
        expectedChunks = 1
        if self.__chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / self.__chunkSize)
        # get upload parameters
        saveFilePath = None
        chunkIndex = 0
        uploadId = None
        parameters = {
            "repositoryType": self.__repositoryType,
            "depId": self.__depId,
            "contentType": self.__contentType,
            "milestone": self.__milestone,
            "partNumber": self.__partNumber,
            "contentFormat": self.__contentFormat,
            "version": self.__version,
            "allowOverwrite": False,
            "resumable": False
        }
        url = os.path.join(self.__baseUrl, "getUploadParameters")
        response = None
        with TestClient(app) as client:
            response = client.get(
                url, params=parameters, headers=self.__headerD, timeout=None
            )
        if not response or not response.status_code:
            logging.error("error - no response")
            sys.exit()
        self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                saveFilePath = result["filePath"]
                chunkIndex = int(result["chunkIndex"])
                uploadId = result["uploadId"]
                if chunkIndex > 0:
                    logging.info(f"detected upload with chunk index {chunkIndex}")
        self.assertTrue(saveFilePath is not None)
        self.assertTrue(uploadId is not None)
        self.assertTrue(chunkIndex == 0)
        if not saveFilePath:
            logging.error("No file path was formed")
            return None
        if not uploadId:
            logging.error("No upload id was formed")
            return None

        # chunk file and upload
        mD = {
            # chunk parameters
            "chunkSize": self.__chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # upload file parameters
            "uploadId": uploadId,
            "hashType": self.__hashType,
            "hashDigest": fullTestHash,
            # save file parameters
            "filePath": saveFilePath,
            "decompress": False,
            "allowOverwrite": True,
            "resumable": False
        }
        offset = chunkIndex * self.__chunkSize
        response = None
        with open(self.__dataFile, "rb") as of:
            of.seek(offset)
            url = os.path.join(self.__baseUrl, "upload")
            for _ in range(chunkIndex, mD["expectedChunks"]):
                packetSize = min(
                    int(fileSize) - (int(mD["chunkIndex"]) * int(self.__chunkSize)),
                    int(self.__chunkSize),
                )
                logging.debug("packet size %s chunk %s expected %s", packetSize, mD['chunkIndex'], expectedChunks)
                with TestClient(app) as client:
                    response = client.post(
                        url,
                        data=deepcopy(mD),
                        headers=self.__headerD,
                        files={"chunk": of.read(packetSize)},
                        timeout=None,
                    )
                if not response or not response.status_code:
                    logging.error(
                        "Status code %r with text %r ...terminating",
                        response.status_code,
                        response.text,
                    )
                    break
                self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
                mD["chunkIndex"] += 1
        return response

    def testContentTypeFormat(self):
        contentTypeList = [None, "model", "map-model-fsc", "badType"]
        contentFormatList = [None, "pdbx", "xml", "badFormat"]
        validCombinationList = [("model", "pdbx"), ("map-model-fsc", "xml")]
        for contentType in contentTypeList:
            for contentFormat in contentFormatList:
                result = UploadUtility().checkContentTypeFormat(contentType, contentFormat)
                if (contentType, contentFormat) in validCombinationList:
                    self.assertTrue(result, "error - result false for %s %s" % (contentType, contentFormat))
                else:
                    self.assertFalse(result, "error - result true for %s %s" % (contentType, contentFormat))

def upload_tests():
    suite = unittest.TestSuite()
    suite.addTest(UploadTest("testSimpleUpload"))
    suite.addTest(UploadTest("testSimpleUpdate"))
    suite.addTest(UploadTest("testContentTypeFormat"))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(upload_tests())
