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
import random
import resource
import string
import time
import unittest

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
from rcsb.utils.io.FileUtil import FileUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FileDownloadTests(unittest.TestCase):

    def setUp(self):
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__dataPath = os.path.join(HERE, "data")
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")
        self.__downloadFilePath = os.path.join(self.__dataPath, "downloadFile.dat")
        # Note - testConfigProvider() must precede this test to install a bootstrap configuration file
        cP = ConfigProvider(self.__configFilePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__configFilePath).createToken({}, subject)}
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

    def testSimpleDownload(self):
        """Test - simple file download"""
        testFilePath = self.__testFilePath

        for endPoint in ["download"]:
            startTime = time.time()
            try:
                mD = {
                    "repositoryType": "archive",
                    "depId": "D_1000000001",
                    "contentType": "model",
                    "contentFormat": "pdbx",
                    "partNumber": 1,
                    "version": 1,
                    "hashType": "MD5",
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
