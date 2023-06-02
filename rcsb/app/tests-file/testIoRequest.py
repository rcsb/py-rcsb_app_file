##
# File:    testIoRequest.py
# Author:  Dennis Piehl
# Date:    24-May-2022
# Version: 0.001
#
# Update: James Smith 2023
#
#
##

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
from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class IoRequestTests(unittest.TestCase):

    def setUp(self):
        cP = ConfigProvider()
        self.__configFilePath = cP.getConfigFilePath()
        self.__chunkSize = cP.get("CHUNK_SIZE")
        self.__hashType = cP.get("HASH_TYPE")
        self.__dataPath = cP.get("REPOSITORY_DIR_PATH")
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
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken().createToken({}, subject)}
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


    def testCopyFile(self):
        endPoint = "copy-file"
        try:
            # Copy file from one repositoryType to another
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                "contentTypeSource": "model",
                "milestoneSource": "",
                "partNumberSource": 1,
                "contentFormatSource": "pdbx",
                "versionSource": 1,
                #
                "repositoryTypeTarget": self.__repositoryType2,
                "depIdTarget": "D_1000000001",
                "contentTypeTarget": "model",
                "milestoneTarget": "",
                "partNumberTarget": 1,
                "contentFormatTarget": "pdbx",
                "versionTarget": 1,
                #
                "overwrite": False
            }
            with TestClient(app) as client:
                response = client.post("/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCopyDir(self):
        endpoint = "copy-dir"
        try:
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                "contentTypeSource": "model",
                "milestoneSource": "",
                "partNumberSource": 1,
                "contentFormatSource": "pdbx",
                "versionSource": 1,
                #
                "repositoryTypeTarget": self.__repositoryType2,
                "depIdTarget": "D_1000000001",
                "contentTypeTarget": "model",
                "milestoneTarget": "",
                "partNumberTarget": 1,
                "contentFormatTarget": "pdbx",
                "versionTarget": 1,
                #
                "overwrite": False
            }
            with TestClient(app) as client:
                response = client.post("/%s" % endpoint, data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testMoveFile(self):
        endPoint = "move-file"
        try:
            # Move file from one repositoryType to another
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                "contentTypeSource": "model",
                "milestoneSource": "",
                "partNumberSource": 1,
                "contentFormatSource": "pdbx",
                "versionSource": 1,
                #
                "repositoryTypeTarget": self.__repositoryType2,
                "depIdTarget": "D_1000000001",
                "contentTypeTarget": "model",
                "milestoneTarget": "",
                "partNumberTarget": 1,
                "contentFormatTarget": "pdbx",
                "versionTarget": 1,
                #
                "overwrite": "True"
            }
            with TestClient(app) as client:
                response = client.post("/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCompressDir(self):
        try:
            # First create a copy of one archive directory
            endPoint = "copy-dir"
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                #
                "repositoryTypeTarget": self.__repositoryType,
                "depIdTarget": "D_1000000002",
            }
            with TestClient(app) as client:
                response = client.post("/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200)
            # Next compress the copied directory
            endPoint = "compress-dir"
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1000000002",
            }
            with TestClient(app) as client:
                response = client.post("/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCompressDirPath(self):
        try:
            # First create a copy of one archive directory
            endPoint = "copy-dir"
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                #
                "repositoryTypeTarget": self.__repositoryType,
                "depIdTarget": "D_1000000002",
            }
            with TestClient(app) as client:
                response = client.post("/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200)
            # Next compress the copied directory
            endPoint = "compress-dir-path"
            mD = {
                "dirPath": os.path.join(self.__unitTestFolder, "D_1000000002")
            }
            with TestClient(app) as client:
                response = client.post("/%s" % endPoint, data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testDecompressDir(self):
        try:
            mD = {"repositoryType": self.__repositoryType, "depId": "D_1000000001"}
            with TestClient(app) as client:
                response = client.post("/compress-dir", data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200)
            with TestClient(app) as client:
                response = client.post("/decompress-dir", data=mD, headers=self.__headerD)
                logger.info("file status %r", response.status_code)
                self.assertTrue(response.status_code == 200)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()




def tests():
    suites = unittest.TestSuite()
    suites.addTest(IoRequestTests("testCopyFile"))
    suites.addTest(IoRequestTests("testMoveFile"))
    suites.addTest(IoRequestTests("testCompressDir"))
    suites.addTest(IoRequestTests("testCompressDirPath"))
    suites.addTest(IoRequestTests("testDecompressDir"))
    return suites


if __name__ == "__main__":
    suite = tests()
    unittest.TextTestRunner(verbosity=2).run(suite)
