##
# File:    testPathRequest.py
# Author:  Dennis Piehl
# Date:    24-May-2022
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for file/directory path API endpoints.

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

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output"))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "tests-file", "test-data", "config", "config.yml"))
# Use custom cache and config path for this set of tests
os.environ["CACHE_PATH"] = os.path.join(HERE, "test-output")
os.environ["CONFIG_FILE"] = os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml")
# os.environ["CONFIG_FILE"] = os.path.join(TOPDIR, "rcsb", "app", "tests-file", "test-data", "config", "config.yml")

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.FileUtil import FileUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PathRequestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Fixture to create file test data"""
        cls.__dataPath = os.path.join(HERE, "test-data")
        cls.__cachePath = os.environ.get("CACHE_PATH")
        cls.__repoTestPath = os.path.join(cls.__cachePath, "data", "repository", "archive")
        logger.info("cls.__repoTestPath %s", cls.__repoTestPath)

        fU = FileUtil()
        cls.__testFilePath = os.path.join(cls.__dataPath, "example-data.cif")
        for fn in ["example-data.cif"]:  # Only needed for ConfigProvider init
            fU.put(cls.__testFilePath, os.path.join(cls.__cachePath, fn))
        #
        PathRequestTests.__repoFixture()

    @classmethod
    def __repoFixture(cls):
        ctFmtTupL = [
            ("model", "cif"),
            ("sf-convert-report", "cif"),
            ("sf-convert-report", "txt"),
            ("sf-upload-convert", "cif"),
            ("sf-upload", "cif"),
            ("sf", "cif"),
        ]
        # Example - D_1000258919_model_P1.cif.V1
        for depId in ["D_1000000001", "D_2000000001"]:
            dirPath = os.path.join(cls.__repoTestPath, depId)
            FileUtil().mkdir(dirPath)
            for pNo in ["P1", "P2"]:
                for contentType, fmt in ctFmtTupL[:6]:
                    for vS in ["V1", "V2"]:
                        fn = depId + "_" + contentType + "_" + pNo + "." + fmt + "." + vS
                        pth = os.path.join(dirPath, fn)
                        FileUtil().put(cls.__testFilePath, pth)

    def setUp(self):
        self.__configFilePath = os.environ.get("CONFIG_FILE")

        # Note - testConfigProvider() must (maybe?) precede this test to install a bootstrap configuration file
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
                "depId": "D_2000000001",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            #
            # Next test for file that DOESN'T exists
            mD = {
                "depId": "D_1234567890",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
            path = os.path.join(self.__repoTestPath, "D_2000000001", "D_2000000001_model_P1.cif.V1")
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for file that DOESN'T exists
            path = os.path.join(self.__repoTestPath, "D_1234567890", "D_1234567890_model_P1.cif.V1")
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 404)
            #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testDirExists(self):
        """Test - dir exists"""
        startTime = time.time()
        try:
            # First test for dir that actually exists using explicit dirPath (created in fixture above)
            endPoint = "path-exists"
            path = os.path.join(self.__repoTestPath, "D_2000000001")
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that actually exists using standard params
            endPoint = "dir-exists"
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"depId": "D_2000000001", "repositoryType": "archive"}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that DOESN'T exists using standard params
            endPoint = "dir-exists"
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"depId": "D_1234567890", "repositoryType": "archive"}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 404)
            #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testListDir(self):
        """Test - list dir"""
        startTime = time.time()
        try:
            # First test for dir that actually exists (created in fixture above), given a specific dirPath
            endPoint = "list-dirpath"
            path = os.path.join(self.__repoTestPath, "D_2000000001")
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params={"dirPath": path}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that actually exists (created in fixture above), given depId and repositoryType
            endPoint = "list-dir"
            mD = {
                "depId": "D_2000000001",
                "repositoryType": "onedep-archive",
            }
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that DOESN'T exists
            endPoint = "list-dirpath"
            path = os.path.join(self.__repoTestPath, "D_1234567890")
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params={"dirPath": path}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
                "depId": "D_1000000001",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
            }
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCopyFile(self):
        """Test - file copy"""
        endPoint = "copy-file"
        startTime = time.time()
        try:
            # Copy file from one repositoryType to another
            mD = {
                "depIdSource": "D_1000000001",
                "repositoryTypeSource": "onedep-archive",
                "contentTypeSource": "model",
                "contentFormatSource": "pdbx",
                "partNumberSource": 1,
                #
                "depIdTarget": "D_1000000001",
                "repositoryTypeTarget": "onedep-deposit",
                "contentTypeTarget": "model",
                "contentFormatTarget": "pdbx",
                "partNumberTarget": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testMoveFile(self):
        """Test - move file"""
        endPoint = "move-file"
        startTime = time.time()
        try:
            # Move file from one repositoryType to another
            mD = {
                "depIdSource": "D_2000000001",
                "repositoryTypeSource": "onedep-archive",
                "contentTypeSource": "model",
                "contentFormatSource": "pdbx",
                "partNumberSource": 1,
                "versionSource": 1,
                "milestoneSource": "",
                "depIdTarget": "D_3000000001",
                "repositoryTypeTarget": "onedep-archive",
                "contentTypeTarget": "model",
                "contentFormatTarget": "pdbx",
                "partNumberTarget": 2,
                "versionTarget": 2,
                "milestoneTarget": ""
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCompressDir(self):
        """Test - compress dir"""
        endPoint = "compress-dir"
        startTime = time.time()
        try:
            # First create a copy of one archive directory
            mD = {
                "depIdSource": "D_1000000001",
                "repositoryTypeSource": "onedep-archive",
                "contentTypeSource": "model",
                "contentFormatSource": "pdbx",
                "partNumberSource": 1,
                #
                "depIdTarget": "D_1000000002",
                "repositoryTypeTarget": "onedep-archive",
                "contentTypeTarget": "model",
                "contentFormatTarget": "pdbx",
                "partNumberTarget": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % "copy-file", params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next compress the copied directory
            mD = {
                "depId": "D_1000000002",
                "repositoryType": "onedep-archive",
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def pathRequestTestSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PathRequestTests("testFileExists"))
    suiteSelect.addTest(PathRequestTests("testPathExists"))
    suiteSelect.addTest(PathRequestTests("testDirExists"))
    suiteSelect.addTest(PathRequestTests("testListDir"))
    suiteSelect.addTest(PathRequestTests("testLatestFileVersion"))
    suiteSelect.addTest(PathRequestTests("testCopyFile"))
    suiteSelect.addTest(PathRequestTests("testMoveFile"))
    suiteSelect.addTest(PathRequestTests("testCompressDir"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = pathRequestTestSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
