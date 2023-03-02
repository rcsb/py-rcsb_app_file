##
# File:    testPathRequest.py
# Author:  Dennis Piehl
# Date:    24-May-2022
# Version: 0.001
#
# Update: James Smith 2023
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
import shutil

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CONFIG_FILE"] = os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml")

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PathRequestTests(unittest.TestCase):

    def setUp(self):
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(self.__configFilePath)
        self.__chunkSize = cP.get("CHUNK_SIZE")
        self.__hashType = cP.get("HASH_TYPE")
        self.__dataPath = cP.get("REPOSITORY_DIR_PATH")  # os.path.join(HERE, "data")
        self.__repositoryType = "unit-test"
        self.__repositoryType2 = "test"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        self.__testFolder = os.path.join(self.__dataPath, self.__repositoryType2)
        self.__repoTestPath = os.path.join(self.__dataPath, self.__repositoryType)
        self.__repoTestFile1 = os.path.join(self.__repoTestPath, "D_1000000001", "D_1000000001_model_P1.cif.V1")
        if not os.path.exists(self.__repoTestFile1):
            os.makedirs(os.path.dirname(self.__repoTestFile1), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repoTestFile1, "wb") as out:
                out.write(os.urandom(nB))
        self.__repoTestFile2 = os.path.join(self.__repoTestPath, "D_2000000001", "D_2000000001_model_P1.cif.V1")
        if not os.path.exists(self.__repoTestFile2):
            os.makedirs(os.path.dirname(self.__repoTestFile2), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repoTestFile2, "wb") as out:
                out.write(os.urandom(nB))
        self.__repoTestPath2 = os.path.join(self.__dataPath, self.__repositoryType2)
        self.__repoTestFile3 = os.path.join(self.__repoTestPath2, "D_1000000001", "D_1000000001_model_P1.cif.V1")
        self.__repoTestFile4 = os.path.join(self.__repoTestPath2, "D_2000000001", "D_2000000001_model_P1.cif.V1")
        self.__repoTestFile5 = os.path.join(self.__repoTestPath, "D_1000000002.tar.gz")

        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__configFilePath).createToken({}, subject)}
        logger.info("header %r", self.__headerD)
        self.__startTime = time.time()
        #
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if os.path.exists(self.__repoTestFile1):
            os.unlink(self.__repoTestFile1)
        if os.path.exists(self.__repoTestFile2):
            os.unlink(self.__repoTestFile2)
        if os.path.exists(self.__repoTestFile3):
            os.unlink(self.__repoTestFile3)
        if os.path.exists(self.__repoTestFile4):
            os.unlink(self.__repoTestFile4)
        if os.path.exists(self.__repoTestFile5):
            os.unlink(self.__repoTestFile5)
        # warning - do not delete the repository/data folder for production, just the unit-test or test folder within that
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        if os.path.exists(self.__testFolder):
            shutil.rmtree(self.__testFolder)
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
            mD = {
                "depId": "D_2000000001",
                "repositoryType": self.__repositoryType,
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
                #
            #
            # Next test for file that DOESN'T exists
            mD = {
                "depId": "D_1234567890",
                "repositoryType": self.__repositoryType,
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for file that DOESN'T exists
            path = os.path.join(self.__repoTestPath, "D_1234567890", "D_1234567890_model_P1.cif.V1")
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
            # First test for dir that actually exists using explicit dirPath
            endPoint = "path-exists"
            path = os.path.join(self.__repoTestPath, "D_2000000001")
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that actually exists using standard params
            endPoint = "dir-exists"
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"depId": "D_2000000001", "repositoryType": self.__repositoryType}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that DOESN'T exists using standard params
            endPoint = "dir-exists"
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params={"depId": "D_1234567890", "repositoryType": self.__repositoryType}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that actually exists (created in fixture above), given depId and repositoryType
            endPoint = "list-dir"
            mD = {
                "depId": "D_2000000001",
                "repositoryType": self.__repositoryType,
            }
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next test for dir that DOESN'T exists
            endPoint = "list-dirpath"
            path = os.path.join(self.__repoTestPath, "D_1234567890")
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params={"dirPath": path}, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
                "repositoryType": self.__repositoryType,
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
            }
            with TestClient(app) as client:
                response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
                "repositoryTypeSource": self.__repositoryType,
                "contentTypeSource": "model",
                "contentFormatSource": "pdbx",
                "partNumberSource": 1,
                #
                "depIdTarget": "D_1000000001",
                "repositoryTypeTarget": self.__repositoryType2,
                "contentTypeTarget": "model",
                "contentFormatTarget": "pdbx",
                "partNumberTarget": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
                "depIdSource": "D_1000000001",
                "repositoryTypeSource": self.__repositoryType,
                "contentTypeSource": "model",
                "contentFormatSource": "pdbx",
                "partNumberSource": 1,
                "versionSource": 1,
                "milestoneSource": "",
                "depIdTarget": "D_2000000001",
                "repositoryTypeTarget": self.__repositoryType2,
                "contentTypeTarget": "model",
                "contentFormatTarget": "pdbx",
                "partNumberTarget": 1,
                "versionTarget": 1,
                "milestoneTarget": ""
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
                "repositoryTypeSource": self.__repositoryType,
                "contentTypeSource": "model",
                "contentFormatSource": "pdbx",
                "partNumberSource": 1,
                #
                "depIdTarget": "D_1000000002",
                "repositoryTypeTarget": self.__repositoryType,
                "contentTypeTarget": "model",
                "contentFormatTarget": "pdbx",
                "partNumberTarget": 1,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % "copy-file", params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
                self.assertTrue(response.status_code == 200)
                logger.info("Content length (%d)", len(response.content))
            #
            # Next compress the copied directory
            mD = {
                "depId": "D_1000000002",
                "repositoryType": self.__repositoryType,
            }
            with TestClient(app) as client:
                response = client.post("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response status code %r", response.status_code)
                # logger.info("response %r %r %r", response.status_code, response.reason_phrase, response.content)
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
    suiteSelect.addTest(PathRequestTests("testCompressDir"))
    suiteSelect.addTest(PathRequestTests("testMoveFile"))  # deletes a file that other tests may rely on so must go last
    return suiteSelect


if __name__ == "__main__":
    mySuite = pathRequestTestSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
