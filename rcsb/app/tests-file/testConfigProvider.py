##
# File:    testConfigProvider.py
# Author:  J. Westbrook
# Date:    24-Aug-2020
# Version: 0.001
#
# Update: James Smith 2023
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
import rcsb.app.config.setConfig
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file import __version__

# HERE = os.path.abspath(os.path.dirname(__file__))
# TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ConfigProviderTests(unittest.TestCase):
    def setUp(self):
        self.__startTime = time.time()
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(self.__configFilePath)
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
            raise Exception("Could not make config dictionary")

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
        cP = ConfigProvider(self.__configFilePath)
        for ky, vl in self.__cD.items():
            tv = cP.get(ky)
            self.assertEqual(tv, vl)

        cP = ConfigProvider(self.__configFilePath)
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
