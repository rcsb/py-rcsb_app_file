##
# File:    testClientUtils.py
# Author:  Dennis Piehl
# Date:    10-June-2022
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for client utility wrapper for making API calls.

"""

__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest
import asyncio
import json
from rcsb.app.file import __version__
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.app.file.ClientUtils import ClientUtils

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.propagate = False


class ClientUtilsTests(unittest.TestCase):
    def setUp(self):
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")
        # self.__testFilePath = os.path.join(self.__dataPath, "bigFile.txt.5gb")
        self.__testFileDownloadPath = os.path.join(
            HERE, "test-output", "example-data-download.cif"
        )
        self.__cachePath = os.environ.get(
            "CACHE_PATH", os.path.join(HERE, "test-output", "CACHE")
        )
        self.__configFilePath = os.environ.get(
            "CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml")
        )
        #
        fU = FileUtil()
        for fn in ["example-data.cif"]:  # Only needed for ConfigProvider init
            fU.put(self.__testFilePath, os.path.join(self.__cachePath, fn))
        #
        self.__cU = ClientUtils(
            cachePath=self.__cachePath,
            configFilePath=self.__configFilePath,
            hostAndPort="http://0.0.0.0:8000",
        )
        #
        self.__startTime = time.time()
        #
        logger.debug("Running tests on version %s", __version__)
        logger.info(
            "Starting %s at %s",
            self.id(),
            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
        )

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10**6, unitS)
        endTime = time.time()
        logger.info(
            "Completed %s at %s (%.4f seconds)",
            self.id(),
            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
            endTime - self.__startTime,
        )

    def testClientUtils(self):
        """Test - file upload, multipart upload, and file download"""
        try:
            asyncio.run(self.__cU.clearKv())
            # Test single-file multi-part upload
            logger.info("Starting upload of file %s", self.__testFilePath)
            # sessionId = asyncio.run(self.__cU.getSession())
            uploadIds = []
            chunkSize = 2500
            startTime = time.time()
            responses = asyncio.run(
                self.__cU.upload(
                    [
                        {
                            # upload file parameters
                            "filePath": self.__testFilePath,
                            "copyMode": "native",
                            "uploadId": None,
                            # chunk parameters
                            "chunkSize": chunkSize,
                            "chunkIndex": None,
                            "chunkOffset": None,
                            "expectedChunks": None,
                            "chunkMode": "async",
                            # save file parameters
                            "repositoryType": "onedep-archive",
                            "depId": "D_3000000000",
                            "contentType": "model",
                            "milestone": "release",
                            "partNumber": 1,
                            "contentFormat": "pdbx",
                            "version": "9",
                            "allowOverwrite": True,
                            "emailAddress": None,
                        }
                    ]
                )
            )
            logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
            """ even for one file, upload function returns a list of size one with one response 
                since same function is reused for multi-file uploads
            """
            for res in responses:
                text = json.loads(res.text)
                uploadIds.append(text["uploadId"])
            #
            # Test *concurrency* for multiple single-file single-part uploads
            logger.info("Starting concurrent single-file uploads")
            startTime = time.time()
            tL = []
            numTasks = 8
            for i in range(numTasks):
                tL.append(
                    {
                        "filePath": self.__testFilePath,
                        "depId": "D_400000000" + str(i),
                        "repositoryType": "onedep-archive",
                        "contentType": "model",
                        "contentFormat": "pdbx",
                        "partNumber": 1,
                        "version": "9",
                        "copyMode": "native",
                        "allowOverwrite": True,
                        "chunkSize": chunkSize,
                        "milestone": "upload"
                    }
                )
            taskL = asyncio.run(self.__cU.upload(tL))
            logger.info("single-file upload taskL: %r", taskL)
            logger.info(
                "Completed concurrent upload (%.4f seconds)", time.time() - startTime
            )
            for task in taskL:
                text = json.loads(task.text)
                uploadIds.append(text["uploadId"])
            #
            # Test single-file multipart upload
            logger.info("Starting multipart-upload of file %s", self.__testFilePath)
            startTime = time.time()
            responses = asyncio.run(
                self.__cU.upload(
                    [
                        {
                            "filePath": self.__testFilePath,
                            "depId": "D_5000000000",
                            "repositoryType": "onedep-archive",
                            "contentType": "model",
                            "contentFormat": "pdbx",
                            "partNumber": 1,
                            "version": "9",
                            "copyMode": "native",
                            "allowOverwrite": True,
                            "chunkSize": chunkSize,
                            "milestone": "upload"
                        }
                    ]
                )
            )
            #
            logger.info(
                "Completed multipart upload (%.4f seconds) %s",
                time.time() - startTime,
                str(responses),
            )
            for res in responses:
                text = json.loads(res.text)
                uploadIds.append(text["uploadId"])
            #
            # Test multi-file multipart upload
            logger.info(
                "Starting multi-file multipart-upload of file %s", self.__testFilePath
            )
            startTime = time.time()
            data = []
            for part in range(1, 4):
                data.append(
                    {
                        "filePath": self.__testFilePath,
                        "depId": "D_5000000001",
                        "repositoryType": "onedep-archive",
                        "contentType": "model",
                        "contentFormat": "pdbx",
                        "partNumber": part,
                        "version": "9",
                        "copyMode": "native",
                        "allowOverwrite": True,
                        "chunkSize": chunkSize,
                        "milestone": "upload"
                    }
                )
            results = asyncio.run(self.__cU.upload(data))
            for result in results:
                print(f"multi-file multipart result {result}")
                text = json.loads(result.text)
                uploadIds.append(text["uploadId"])
            #
            logger.info(
                "Completed multi-file multipart upload (%.4f seconds)",
                time.time() - startTime,
            )
            #
            # Test file download
            logger.info(
                "Starting download of last uploaded file to %s",
                self.__testFileDownloadPath,
            )
            startTime = time.time()
            asyncio.run(
                self.__cU.download(
                    fileDownloadPath=self.__testFileDownloadPath,
                    depId="D_4000000000",
                    repositoryType="onedep-archive",
                    contentType="model",
                    contentFormat="pdbx",
                    partNumber=1,
                    version="9",
                    milestone=""
                )
            )
            # logger.info("Completed download (%.4f seconds)", time.time() - startTime)
            # ok = asyncio.run(self.__cU.clearSession(uploadIds))
            # logger.info("Removed session with status %r", ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def clientUtilTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ClientUtilsTests("testClientUtils"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = clientUtilTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
