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

from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.client.ClientUtils import ClientUtils

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FileUpdateTests(unittest.TestCase):
    def setUp(self):
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__cU = ClientUtils(unit_test=True)
        self.__cP = ConfigProvider(self.__configFilePath)
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__hashType = self.__cP.get("HASH_TYPE")
        self.__dataPath = self.__cP.get("REPOSITORY_DIR_PATH")  # os.path.join(HERE, "data")
        self.__repositoryType = "unit-test"
        self.__depId = "D_2000000001"
        self.__contentType = "model"
        self.__milestone = ""
        self.__partNumber = 1
        self.__contentFormat = "pdbx"
        self.__version = 1
        self.__decompress = False
        self.__allowOverwrite = True
        self.__resumable = False
        self.__repositoryFilePath = os.path.join(self.__dataPath, self.__repositoryType, self.__depId, f"{self.__depId}_{self.__contentType}_P{self.__partNumber}.cif.V{self.__version}")
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        if not os.path.exists(self.__repositoryFilePath):
            os.makedirs(os.path.dirname(self.__repositoryFilePath), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFilePath, "wb") as out:
                out.write(os.urandom(nB))
        self.__readFilePath = os.path.join(self.__dataPath, "testFile.dat")
        if not os.path.exists(self.__readFilePath):
            os.makedirs(os.path.dirname(self.__readFilePath), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__readFilePath, "wb") as out:
                out.write(os.urandom(nB))
        subject = self.__cP.get("JWT_SUBJECT")
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
        # warning - do not delete the data/repository folder for production, just the unit-test folder within that folder
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleUpdate(self):
        """Test - simple file overwrite"""

        # Update file content

        endPoint = "upload"
        responseCode = 200

        startTime = time.time()
        try:
            response = self.__cU.upload(self.__readFilePath, self.__repositoryType, self.__depId, self.__contentType, self.__milestone, self.__partNumber, self.__contentFormat, self.__version, self.__decompress, self.__allowOverwrite, self.__resumable)
            self.assertTrue(response.status_code == responseCode)
            rD = response.json()
            logger.info("rD %r", rD.items())
            if responseCode == 200:
                self.assertTrue(rD["success"])
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
