##
# File:    testConfigProvider.py
# Author:  J. Westbrook
# Date:    24-Aug-2020
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for configuration utilities.

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

from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file import __version__

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CONFIG_FILE"] = "./rcsb/app/config/config.yml"
# os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("rcsb", "app", "tests-file", "test-data", "data"))
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "tests-file", "test-data", "config", "config.yml"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ConfigProviderTests(unittest.TestCase):
    def setUp(self):
        self.__startTime = time.time()
        self.__cachePath = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        logger.info("Using cache path %r", self.__cachePath)
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        #
        self.__cD = {}
        if self.__configFilePath:
            try:
                self.__cD = cP.getConfig()
            except Exception as e:
                logger.info("Unable to getConfig with exception %s", str(e))
            #
        #
        if not self.__cD:
            self.__cD = {
                "JWT_SUBJECT": "aTestSubject",
                "JWT_ALGORITHM": "HS256",
                "JWT_SECRET": "aTestSecret",
                "SESSION_DIR_PATH": os.path.join(self.__cachePath, "sessions"),
                "REPOSITORY_DIR_PATH": os.path.join(self.__cachePath, "repository"),
                "SHARED_LOCK_PATH": os.path.join(self.__cachePath, "shared-locks"),
            }
            # cP.setConfig(configData=self.__cD)
        #
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testConfigAccessors(self):
        """Test -configuration accessors"""
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        for ky, vl in self.__cD.items():
            tv = cP.get(ky)
            self.assertEqual(tv, vl)

        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        for ky, vl in self.__cD.items():
            tv = cP.get(ky)
            self.assertEqual(tv, vl)


def configAccessorsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ConfigProviderTests("testConfigAccessors"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = configAccessorsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
