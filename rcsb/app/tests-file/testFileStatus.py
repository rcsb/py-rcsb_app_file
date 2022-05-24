##
# File:    testFileStatus.py
# Author:  Dennis Piehl
# Date:    24-May-2022
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for file status API.

"""

__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import platform
import random
import resource
import string
import time
import unittest

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FileStatusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Fixture to create file test data"""
        dataPath = os.path.join(HERE, "test-data")
        cachePath = os.environ.get("CACHE_PATH")
        # sessionPath = os.path.join(cachePath, "sessions")
        repoTestPath = os.path.join(dataPath, "data", "repository", "archive")

        fU = FileUtil()
        # fU.mkdir(sessionPath)
        testFilePath = os.path.join(dataPath, "example-data.cif")
        for fn in ["example-data.cif"]:  # Only needed for ConfigProvider init
            fU.put(testFilePath, os.path.join(cachePath, fn))
        #
        # nB = 2500000
        # testFilePath = os.path.join(sessionPath, "testFile.dat")
        # with open(testFilePath, "w", encoding="utf-8") as ofh:
        #     ofh.write("".join(random.choices(string.ascii_uppercase + string.digits, k=nB)))
        # #
        # testFilePath = os.path.join(dataPath, "example-data.cif")
        FileStatusTests.__repoFixture(repoTestPath, testFilePath)

    @classmethod
    def __repoFixture(cls, repoPath, testFilePath):
        ctFmtTupL = [
            ("model", "cif"),
            ("sf-convert-report", "cif"),
            ("sf-convert-report", "txt"),
            ("sf-upload-convert", "cif"),
            ("sf-upload", "cif"),
            ("sf", "cif"),
        ]
        # Example - D_1000258919_model_P1.cif.V1
        for idCode in ["D_2000000001"]:
            dirPath = os.path.join(repoPath, idCode)
            FileUtil().mkdir(dirPath)
            for pNo in ["P1", "P2"]:
                for contentType, fmt in ctFmtTupL[:6]:
                    for vS in ["V1", "V2"]:
                        fn = idCode + "_" + contentType + "_" + pNo + "." + fmt + "." + vS
                        pth = os.path.join(dirPath, fn)
                        FileUtil().put(testFilePath, pth)

    def setUp(self):
        self.__cachePath = os.environ.get("CACHE_PATH")
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__repoTestPath = os.path.join(self.__cachePath, "repository", "archive")
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")
        # self.__downloadFilePath = os.path.join(self.__cachePath, "downloadFile.dat")

        # if not os.environ.get("REPOSITORY_PATH", None):
        #     os.environ["REPOSITORY_PATH"] = self.__repoTestPath
        # else:
        #     logger.info("Using REPOSITORY_PATH setting from environment %r", os.environ.get("REPOSITORY_PATH"))

        # Note - testConfigProvider() must precede this test to install a bootstrap configuration file
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath, self.__configFilePath).createToken({}, subject)}
        logger.info("header %r", self.__headerD)
        self.__startTime = time.time()
        #
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testFileExists(self):
        """Test - file exists"""
        endPoint = "file-exists"
        startTime = time.time()
        try:
            # First test for file that actually exists (created in fixture above)
            mD = {
                "idCode": "D_2000000001",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                # print("RESPONSE", response.text)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            #
            # Next test for file that DOESN'T exists
            mD = {
                "idCode": "D_1234567890",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                # print("RESPONSE", response.text, response.status_code)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                self.assertTrue(response.status_code == 404)
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testPathExists(self):
        """Test - path exists"""
        endPoint = "path-exists"
        startTime = time.time()
        try:
            # First test for file that actually exists (created in fixture above)
            path = "./rcsb/app/tests-file/test-data/data/repository/archive/D_2000000001/D_2000000001_model_P1.cif.V1"
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            #
            # Next test for file that DOESN'T exists
            path = "./rcsb/app/tests-file/test-data/data/repository/archive/D_1234567890/D_1234567890_model_P1.cif.V1"
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                self.assertTrue(response.status_code == 404)
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLatestFileVersion(self):
        """Test - get latest file version"""
        endPoint = "latest-file-version"
        startTime = time.time()
        try:
            # First test for file that actually exists (created in fixture above)
            mD = {
                "idCode": "D_1000000001",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
            }
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                # print("RESPONSE", response.text)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def fileStatusTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileStatusTests("testFileExists"))
    suiteSelect.addTest(FileStatusTests("testPathExists"))
    suiteSelect.addTest(FileStatusTests("testLatestFileVersion"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = fileStatusTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
