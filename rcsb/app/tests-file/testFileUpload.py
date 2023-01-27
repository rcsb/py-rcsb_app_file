##
# File:    testFileUpload.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update: James Smith 2023
#
#
# Notes:
# If running manually with API running with gunicorn (outside of docker), run from ~/rcsb/py-rcsb_app_file:
#   python ./rcsb/app/tests-file/testFileUpload.py
##
"""
Tests for file upload API.

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import sys
import platform
import resource
import time
import unittest
import json
import math
import copy
import io

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
logger.setLevel(logging.INFO)


class FileUploadTests(unittest.TestCase):
    testSliceUpload = False

    def setUp(self):

        self.__dataPath = os.path.join(HERE, "data")
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        logger.info("self.__dataPath %s", self.__dataPath)
        logger.info("self.__configFilePath %s", self.__configFilePath)
        self.__fU = FileUtil()

        self.__repositoryFile1 = os.path.join(self.__dataPath, "repository", "archive", "D_1000000001", "D_1000000001_model_P1.cif.V1")
        self.__repositoryFile2 = os.path.join(self.__dataPath, "repository", "archive", "D_1000000001", "D_1000000001_model_P2.cif.V1")
        self.__repositoryFile3 = os.path.join(self.__dataPath, "repository", "archive", "D_1000000001", "D_1000000001_model_P3.cif.V1")
        if os.path.exists(self.__repositoryFile1):
            os.path.unlink(self.__repositoryFile1)
        if os.path.exists(self.__repositoryFile2):
            os.path.unlink(self.__repositoryFile2)
        if os.path.exists(self.__repositoryFile3):
            os.path.unlink(self.__repositoryFile3)
        os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)

        self.__testFileDatPath = os.path.join(self.__dataPath, "testFile.dat")
        if not os.path.exists(self.__testFileDatPath):
            os.makedirs(os.path.dirname(self.__testFileDatPath), mode=0o757, exist_ok=True)
            nB = 1024 * 1024 * 8
            with open(self.__testFileDatPath, "wb") as out:
                out.write(os.urandom(nB))
        self.__testFileGzipPath = os.path.join(self.__dataPath, "testFile.dat.gz")
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        self.__fU.compress(self.__testFileDatPath, self.__testFileGzipPath)

        cP = ConfigProvider(self.__configFilePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__configFilePath).createToken({}, subject)}
        logger.debug("header %r", self.__headerD)
        #
        self.__startTime = time.time()
        #
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if os.path.exists(self.__repositoryFile1):
            os.unlink(self.__repositoryFile1)
        if os.path.exists(self.__repositoryFile2):
            os.unlink(self.__repositoryFile2)
        if os.path.exists(self.__repositoryFile3):
            os.unlink(self.__repositoryFile3)
        if os.path.exists(self.__testFileDatPath):
            os.unlink(self.__testFileDatPath)
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleUpload(self):
        """Test - basic file upload operations"""
        hashType = testHash = None
        endPoint = "upload"
        hashType = "MD5"
        #
        for testFilePath, copyMode, partNumber, allowOverwrite, responseCode in [
            (self.__testFileDatPath, "native", 1, True, 200),
            (self.__testFileDatPath, "shell", 2, True, 200),
            (self.__testFileDatPath, "native", 1, False, 405),
            (self.__testFileGzipPath, "decompress_gzip", 3, True, 200),
        ]:
            print(f'{copyMode} {partNumber} {allowOverwrite} {responseCode}')
            hD = CryptUtils().getFileHash(testFilePath, hashType=hashType)
            testHash = hD['hashDigest']
            print("testHash", testHash)
            for version in range(1, 2):
                startTime = time.time()
                try:
                    mD = {
                        "hashType": hashType,
                        "hashDigest": testHash,
                        "repositoryType": "onedep-archive",
                        "depId": "D_1000000001",
                        "contentType": "model",
                        "milestone": "",
                        "partNumber": partNumber,
                        "contentFormat": "pdbx",
                        "version": str(version),
                        "copyMode": copyMode,
                        "allowOverwrite": allowOverwrite,
                    }
                    #
                    with TestClient(app) as client:
                        with open(testFilePath, "rb") as ifh:
                            files = {"uploadFile": ifh}
                            logging.warning(f'UPLOADING {version}')
                            response = client.post("/file-v2/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                        print(f'STATUS CODE {response.status_code}')
                        self.assertTrue(response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    #
                    logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()

    def testSequentialUpload(self):
        filePath = None
        parameters = {"repositoryType": "archive",
                      "depId": "D_1000000001",
                      "contentType": "model",
                      "milestone": None,
                      "partNumber": 1,
                      "contentFormat": "pdbx",
                      "version": 1,
                      "allowOverwrite": True
                      }
        with TestClient(app) as client:
            response = client.get(
                "/file-v2/getSaveFilePath",
                params=parameters,
                headers=self.__headerD,
                timeout=None
            )
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                filePath = result["path"]
        self.assertTrue(filePath is not None)

        uploadId = None
        with TestClient(app) as client:
            response = client.get(
                "/file-v2/getNewUploadId",
                headers=self.__headerD,
                timeout=None
            )
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                uploadId = result["id"]
        self.assertTrue(uploadId is not None)

        hashType = "MD5"
        hD = CryptUtils().getFileHash(self.__testFileDatPath, hashType=hashType)
        testHash = hD['hashDigest']
        fileSize = os.path.getsize(self.__testFileDatPath)
        chunkSize = 1024 * 1024 * 1
        expectedChunks = math.ceil(fileSize / chunkSize)
        chunkIndex = 0
        mD = {
            # upload file parameters
            "uploadId": uploadId,
            "hashType": hashType,
            "hashDigest": testHash,
            # chunk parameters
            "chunkSize": chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # save file parameters
            "filePath": filePath,
            "copyMode": "native",
            "allowOverwrite": True
        }
        chunkSize = 1024 * 1024 * 1
        with TestClient(app) as client:
            with open(self.__testFileDatPath, "rb") as infile:
                while chunk := infile.read(chunkSize):
                    files = {"uploadFile": chunk}
                    response = client.post("/file-v2/sequentialUpload", files=files, data=copy.deepcopy(mD), headers=self.__headerD)
                    mD["chunkIndex"] += 1
                    self.assertTrue(response and response.status_code and response.status_code == 200)
        print(f'posted {expectedChunks} chunks')

    def testResumableUpload(self):

        hashType = "MD5"
        hD = CryptUtils().getFileHash(self.__testFileDatPath, hashType=hashType)
        testHash = hD['hashDigest']

        uploadCount = 0
        offset = 0
        parameters = {"repositoryType": "archive",
                      "depId": "D_1000000001",
                      "contentType": "model",
                      "milestone": None,
                      "partNumber": 1,
                      "contentFormat": "pdbx",
                      "hashDigest": testHash
                      }
        with TestClient(app) as client:
            response = client.get(
                "/file-v2/uploadStatus",
                params=parameters,
                headers=self.__headerD,
                timeout=None
            )
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                if not isinstance(result, dict):
                    result = eval(result)
                uploadCount = int(result["uploadCount"])
                print(f'upload count {uploadCount}')
                chunkIndex = uploadCount
                packet_size = min(
                    fileSize - (chunkIndex * chunkSize),
                    chunkSize
                )
                offset = uploadCount * packet_size
                print(f'offset {offset}')

        chunkIndex = uploadCount
        fileSize = os.path.getsize(self.__testFileDatPath)
        chunkSize = 1024 * 1024 * 1
        expectedChunks = math.ceil(fileSize / chunkSize)
        mD = {
            # upload file parameters
            "uploadId": None,
            "hashType": hashType,
            "hashDigest": testHash,
            # chunk parameters
            "chunkSize": chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # save file parameters
            "repositoryType": "archive",
            "depId": "D_1000000001",
            "contentType": "model",
            "milestone": None,
            "partNumber": 1,
            "contentFormat": "pdbx",
            "version": 1,
            "copyMode": "native",
            "allowOverwrite": True,
        }
        chunkSize = 1024 * 1024 * 1
        buffer = io.BytesIO()
        with TestClient(app) as client:
            with open(self.__testFileDatPath, "rb") as infile:
                buffer.seek(offset)
                for x in range(expectedChunks):
                    packet_size = min(
                        fileSize - (chunkIndex * chunkSize),
                        chunkSize,
                    )
                    buffer.truncate(packet_size)
                    buffer.seek(0)
                    buffer.write(infile.read(packet_size))
                    buffer.seek(0)
                    files = {"uploadFile": buffer}
                    response = client.post("/file-v2/resumableUpload", files=files, data=copy.deepcopy(mD),
                                           headers=self.__headerD)
                    mD["chunkIndex"] += 1
                    self.assertTrue(response and response.status_code and response.status_code == 200)
        print(f'posted {expectedChunks} chunks of {expectedChunks}')

        # uploadCount = 0
        # offset = 0
        # parameters = {"repositoryType": "archive",
        #               "depId": "D_1000000001",
        #               "contentType": "model",
        #               "milestone": None,
        #               "partNumber": 1,
        #               "contentFormat": "pdbx",
        #               "hashDigest": testHash
        #               }
        # with TestClient(app) as client:
        #     response = client.get(
        #         "/file-v2/uploadStatus",
        #         params=parameters,
        #         headers=self.__headerD,
        #         timeout=None
        #     )
        # if response.status_code == 200:
        #     result = json.loads(response.text)
        #     if result:
        #         if not isinstance(result, dict):
        #             result = eval(result)
        #         uploadCount = int(result["uploadCount"])
        #         chunkIndex = uploadCount
        #         print(f'upload count {uploadCount}')
        #         packet_size = min(
        #             fileSize - (chunkIndex * chunkSize),
        #             chunkSize
        #         )
        #         offset = uploadCount * packet_size
        #         print(f'offset {offset}')
        #
        # chunkIndex = uploadCount
        # mD["chunkIndex"] = chunkIndex
        #
        # buffer = io.BytesIO()
        # with TestClient(app) as client:
        #     with open(self.__testFileDatPath, "rb") as infile:
        #         buffer.seek(offset)
        #         for x in range(expectedChunks):
        #             packet_size = min(
        #                 fileSize - (chunkIndex * chunkSize),
        #                 chunkSize,
        #             )
        #             buffer.truncate(packet_size)
        #             buffer.seek(0)
        #             buffer.write(infile.read(packet_size))
        #             buffer.seek(0)
        #             files = {"uploadFile": buffer}
        #             response = client.post("/file-v2/resumableUpload", files=files, data=copy.deepcopy(mD),
        #                                    headers=self.__headerD)
        #             mD["chunkIndex"] += 1
        #             self.assertTrue(response and response.status_code and response.status_code == 200)
        # print(f'posted {expectedChunks} chunks starting from {uploadCount}')
        #


def uploadSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileUploadTests("testSimpleUpload"))
    suiteSelect.addTest(FileUploadTests("testSequentialUpload"))
    suiteSelect.addTest(FileUploadTests("testResumableUpload"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = uploadSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
