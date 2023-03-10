##
# File:    testFileDownload.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update: James Smith 2023
#
#
##
"""
Tests for file download API.

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest
import shutil


from fastapi.testclient import TestClient
import rcsb.app.config.setConfig  # noqa: F401 pylint: disable=W0611
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FileDownloadTests(unittest.TestCase):

    def setUp(self):
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(self.__configFilePath)
        self.__chunkSize = cP.get("CHUNK_SIZE")
        self.__hashType = cP.get("HASH_TYPE")
        self.__dataPath = cP.get("REPOSITORY_DIR_PATH")  # os.path.join(HERE, "data")
        self.__repositoryType = "unit-test"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        self.__repositoryFilePath = os.path.join(self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P1.cif.V1")
        if not os.path.exists(self.__repositoryFilePath):
            os.makedirs(os.path.dirname(self.__repositoryFilePath), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFilePath, "wb") as out:
                out.write(os.urandom(nB))
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")
        if not os.path.exists(self.__testFilePath):
            os.makedirs(os.path.dirname(self.__testFilePath), mode=0o757, exist_ok=True)
            nB = 1024 * 1024 * 8
            with open(self.__testFilePath, "wb") as out:
                out.write(os.urandom(nB))
        self.__downloadFilePath = os.path.join(self.__dataPath, "downloadFile.dat")
        if not os.path.exists(self.__downloadFilePath):
            os.makedirs(os.path.dirname(self.__downloadFilePath), mode=0o757, exist_ok=True)
        if os.path.exists(self.__downloadFilePath):
            os.unlink(self.__downloadFilePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__configFilePath).createToken({}, subject)}
        logger.info("header %r", self.__headerD)
        self.__startTime = time.time()
        #
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if os.path.exists(self.__repositoryFilePath):
            os.unlink(self.__repositoryFilePath)
        if os.path.exists(self.__testFilePath):
            os.unlink(self.__testFilePath)
        if os.path.exists(self.__downloadFilePath):
            os.unlink(self.__downloadFilePath)
        # warning - do not delete the data/repository folder for production, just the unit-test folder within that folder
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleDownload(self):
        """Test - simple file download"""
        testFilePath = self.__testFilePath

        for endPoint in ["download"]:
            startTime = time.time()
            try:
                mD = {
                    "repositoryType": self.__repositoryType,
                    "depId": "D_1000000001",
                    "contentType": "model",
                    "contentFormat": "pdbx",
                    "partNumber": 1,
                    "version": 1,
                    "hashType": self.__hashType,
                    "milestone": None
                }
                #
                with TestClient(app) as client:
                    response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                    logger.info("download response status code %r", response.status_code)
                    self.assertTrue(response.status_code == 200)
                    logger.info("Content length (%d)", len(response.content))
                    with open(self.__downloadFilePath, "wb") as ofh:
                        ofh.write(response.content)

                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()


def downloadSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileDownloadTests("testSimpleDownload"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = downloadSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
