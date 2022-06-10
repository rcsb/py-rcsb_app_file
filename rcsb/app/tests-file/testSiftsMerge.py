##
# File:    testMerge.py
# Author:  Connor Parker
# Date:    27-May-2022
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for merging SIFTS data

"""

__docformat__ = "google en"
__author__ = "Connor Parker"
__email__ = "connor.parker@rcsb.org"
__license__ = "Apache 2.0"


import os
import time
import logging
import unittest
import platform
import resource

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
# TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("rcsb", "app", "tests-file", "test-data", "data"))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join("rcsb", "app", "config", "config.yml"))

from fastapi.testclient import TestClient
from rcsb.app.file.main import app
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file import __version__

# os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("rcsb", "app", "tests-file", "test-data", "data"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SIFTSMergeTests(unittest.TestCase):
    def setUp(self):
        self.__cachePath = os.path.join(HERE, "test-data", "data")
        self.__configFilePath = os.environ.get("CONFIG_FILE", os.path.join("rcsb", "app", "config", "config.yml"))

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

    def testSIFTSMerge(self):
        """Test - merge with example SIFTS data"""

        responseCode = 200
        pdbID = "1yy9"
        siftsFilePath = os.path.join(self.__cachePath, "mmcif", pdbID + "_sifts_only.cif.gz")
        logger.info("siftsFilePath %r", siftsFilePath)

        mergeDict = {
            "siftsPath": siftsFilePath,
            "pdbID": pdbID,
        }

        with TestClient(app) as client:
            response = client.post("/file-v1/merge", data=mergeDict, headers=self.__headerD)
            print(response.status_code)
            print(response.text)
            self.assertTrue(response.status_code == responseCode)


def updateSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SIFTSMergeTests("testSIFTSMerge"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = updateSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
