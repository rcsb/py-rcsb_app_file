##
# File:    testPathRequest.py
# Author:  James Smith
# Date:    29-May-2023
# Version: 1.0
#
#
#
##

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import platform
import shutil

import resource
import unittest
from fastapi.testclient import TestClient

from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PathRequestTests(unittest.TestCase):

    def setUp(self):
        self.pathP = PathProvider()
        self.__cP = ConfigProvider()

        self.__configFilePath = self.__cP.getConfigFilePath()
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__hashType = self.__cP.get("HASH_TYPE")

        self.__dataPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__repositoryType = "unit-test"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        logger.info("self.__dataPath %s", self.__unitTestFolder)

        self.__repositoryFile1 = os.path.join(self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P1.cif.V1")
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        self.__repositoryFile2 = os.path.join(self.__unitTestFolder, "D_2000000001", "D_2000000001_model_P1.cif.V1")
        if not os.path.exists(self.__repositoryFile2):
            os.makedirs(os.path.dirname(self.__repositoryFile2), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFile2, "wb") as out:
                out.write(os.urandom(nB))

        subject = self.__cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken().createToken({}, subject)}

        logger.debug("Running tests on version %s", __version__)

    def tearDown(self):
        if os.path.exists(self.__repositoryFile1):
            os.unlink(self.__repositoryFile1)
        if os.path.exists(self.__repositoryFile2):
            os.unlink(self.__repositoryFile2)
        # warning - do not delete the data/repository folder for production, just the unit-test folder within that folder
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        logger.info("Completed")

    def testFilePath(self):
        logger.info("test file path")
        url = "/file-path"
        repoType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1
        parameters = {
            "repositoryType": repoType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version
        }
        # test correct file name returned
        with TestClient(app) as client:
            response = client.get(url, params=parameters, headers=self.__headerD)
            self.assertTrue(response.status_code == 200, f"error - 200 = {response.status_code}")
            results = response.json()
            filePath = results["filePath"]
            self.assertTrue(filePath == self.__repositoryFile1, f"error - returned wrong file path {filePath}")

    def testFileExists(self):
        endPoint = "file-exists"
        try:
            # test response 200
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1000000001",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params=mD, headers=self.__headerD)
                self.assertTrue(response.status_code == 200)
                logger.info("file status response %r", response.status_code)
                logger.info("Content length (%d)", len(response.content))
            # test response 404
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1234567890",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
                "version": 1,
            }
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params=mD, headers=self.__headerD)
                self.assertTrue(response.status_code == 404)
                logger.info("file status response %r", response.status_code)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testPathExists(self):
        endPoint = "path-exists"
        try:
            # test response 200
            path = os.path.join(self.__unitTestFolder, "D_2000000001", "D_2000000001_model_P1.cif.V1")
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("file status response %r", response.status_code)
                self.assertTrue(response.status_code == 200, "error - response %s" % response.status_code)
            # test response 404
            path = os.path.join(self.__unitTestFolder, "D_1234567890", "D_1234567890_model_P1.cif.V1")
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("file status response %r", response.status_code)
                self.assertTrue(response.status_code == 404, "error - response %s" % response.status_code)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testListDir(self):
        endPoint = "list-dir"
        try:
            # test response 200
            mD = {
                "depId": "D_2000000001",
                "repositoryType": self.__repositoryType,
            }
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("dir status response %r", response.status_code)
                self.assertTrue(response.status_code == 200)
            # test response 404
            mD = {
                "depId": "D_2000000002",
                "repositoryType": self.__repositoryType,
            }
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("dir status response status code %r", response.status_code)
                self.assertTrue(response.status_code == 404)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testDirExists(self):
        try:
            # test directory path (without file name)
            endPoint = "path-exists"
            path = os.path.join(self.__unitTestFolder, "D_2000000001")
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params={"path": path}, headers=self.__headerD)
                logger.info("dir status response %r", response.status_code)
                self.assertTrue(response.status_code == 200, "error finding dir path - %s" % response.status_code)
            # test directory parameters with response 200
            endPoint = "dir-exists"
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params={"depId": "D_2000000001", "repositoryType": self.__repositoryType}, headers=self.__headerD)
                logger.info("dir status response %r", response.status_code)
                self.assertTrue(response.status_code == 200, "error in dir exists - %s" % response.status_code)
            # test directory parameters with response 404
            endPoint = "dir-exists"
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params={"depId": "D_1234567890", "repositoryType": self.__repositoryType}, headers=self.__headerD)
                logger.info("dir status response %r", response.status_code)
                self.assertTrue(response.status_code == 404, "error in dir exists - %s" % response.status_code)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testContentTypeFormat(self):
        contentTypeList = [None, "model", "map-model-fsc", "badType"]
        contentFormatList = [None, "pdbx", "xml", "badFormat"]
        validCombinationList = [("model", "pdbx"), ("map-model-fsc", "xml")]
        for contentType in contentTypeList:
            for contentFormat in contentFormatList:
                result = PathProvider().checkContentTypeFormat(contentType, contentFormat)
                if (contentType, contentFormat) in validCombinationList:
                    self.assertTrue(result, "error - result false for %s %s" % (contentType, contentFormat))
                else:
                    self.assertFalse(result, "error - result true for %s %s" % (contentType, contentFormat))

    def testNextVersion(self):
        endPoint = "next-version"
        try:
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1000000001",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
            }
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response %r", response.status_code)
                self.assertTrue(response.status_code == 200)
                results = response.json()
                self.assertTrue(int(results["version"]) == 2, "error - returned wrong file version %s" % results["version"])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLatestVersion(self):
        endPoint = "latest-version"
        try:
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1000000001",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
            }
            with TestClient(app) as client:
                response = client.get("/%s" % endPoint, params=mD, headers=self.__headerD)
                logger.info("file status response %r", response.status_code)
                self.assertTrue(response.status_code == 200)
                results = response.json()
                self.assertTrue(int(results["version"]) == 1, "error - returned wrong file version %s" % results["version"])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()



def tests():
    suite = unittest.TestSuite()
    suite.addTest(PathRequestTests("testFilePath"))
    suite.addTest(PathRequestTests("testFileExists"))
    suite.addTest(PathRequestTests("testPathExists"))
    suite.addTest(PathRequestTests("testListDir"))
    suite.addTest(PathRequestTests("testDirExists"))
    suite.addTest(PathRequestTests("testContentTypeFormat"))
    suite.addTest(PathRequestTests("testNextVersion"))
    suite.addTest(PathRequestTests("testLatestVersion"))
    return suite


if __name__ == "__main__":
    suite = tests()
    unittest.TextTestRunner(verbosity=2).run(suite)
