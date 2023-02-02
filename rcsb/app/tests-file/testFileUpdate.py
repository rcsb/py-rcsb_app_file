##
# File:    testFileUpdate.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update: James Smith 2023
#
#
##
"""
Tests for file update APIs.

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

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FileUpdateTests(unittest.TestCase):
    def setUp(self):
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__dataPath = os.path.join(HERE, "data")
        self.__repositoryFilePath = os.path.join(self.__dataPath, "repository", "archive", "D_2000000001", "D_2000000001_model_P1.cif.V1")
        if not os.path.exists(self.__repositoryFilePath):
            os.makedirs(os.path.dirname(self.__repositoryFilePath), mode=0o757, exist_ok=True)
            nB = 1024 * 1024 * 8
            with open(self.__repositoryFilePath, "wb") as out:
                out.write(os.urandom(nB))
        self.__readFilePath = os.path.join(self.__dataPath, "testFile.dat")
        if not os.path.exists(self.__readFilePath):
            os.makedirs(os.path.dirname(self.__readFilePath), mode=0o757, exist_ok=True)
            nB = 1024 * 1024 * 6
            with open(self.__readFilePath, "wb") as out:
                out.write(os.urandom(nB))
        cP = ConfigProvider(self.__configFilePath)
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
        if os.path.exists(self.__readFilePath):
            os.unlink(self.__readFilePath)
        if os.path.exists(self.__dataPath):
            shutil.rmtree(self.__dataPath)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleUpdate(self):
        """Test - simple file overwrite"""

        # Update file content

        endPoint = "upload"
        hashType = "MD5"
        hD = CryptUtils().getFileHash(self.__readFilePath, hashType=hashType)
        testHash = hD["hashDigest"]
        responseCode = 200

        startTime = time.time()
        try:
            mD = {
                "hashType": hashType,
                "hashDigest": testHash,
                "repositoryType": "onedep-archive",  # First upload into "onedep-archive"
                "depId": "D_2000000001",
                "contentType": "model",
                "milestone": "None",
                "partNumber": 1,
                "contentFormat": "pdbx",
                "version": "1",
                "copyMode": "native",
                "allowOverwrite": True
            }
            #
            with TestClient(app) as client:
                with open(self.__readFilePath, "rb") as ifh:
                    files = {"uploadFile": ifh}
                    response = client.post("/file-v2/%s" % endPoint, files=files, data=mD, headers=self.__headerD)

                self.assertTrue(response.status_code == responseCode)
                rD = response.json()
                logger.info("rD %r", rD.items())
                if responseCode == 200:
                    self.assertTrue(rD["success"])
                #
            #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def updateSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileUpdateTests("testSimpleUpdate"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = updateSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
