##
# File:    testPathUtils.py
# Author:  C. Parker
# Date:    03-Mar-2022
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for path utilities.

"""

__docformat__ = "google en"
__author__ = "Connor Parker"
__email__ = "connor.parker@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest

from rcsb.app.file import __version__
from rcsb.app.file.PathUtils import PathUtils
from rcsb.app.file.ConfigProvider import ConfigProvider

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("rcsb", "app", "data"))
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PathUtilsTests(unittest.TestCase):
    def setUp(self):
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        self.cP = ConfigProvider(cachePath, configFilePath)
        self.pathU = PathUtils(self.cP)
        self.__startTime = time.time()
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testCheckContentTypeFormat(self):
        """Test checkContentTypeFormat"""
        contentType = None
        contentFormat = None
        contentTypeList = [None, "model", "badType"]
        contentFormatList = [None, "pdbx", "badFormat"]

        for contentType in contentTypeList:
            for contentFormat in contentFormatList:
                self.pathU.checkContentTypeFormat(contentType, contentFormat)

    def testGetMimeType(self):
        mimeType = None
        mimeTypeList = ["cif", "pdf", "xml", "json", "txt", "pic", "other"]
        for mimeType in mimeTypeList:
            if mimeType == "cif":
                self.assertEqual(self.pathU.getMimeType(mimeType), "chemical/x-mmcif")
            if mimeType == "pdf":
                self.assertEqual(self.pathU.getMimeType(mimeType), "application/pdf")
            if mimeType == "xml":
                self.assertEqual(self.pathU.getMimeType(mimeType), "application/xml")
            if mimeType == "json":
                self.assertEqual(self.pathU.getMimeType(mimeType), "application/json")
            if mimeType == "txt":
                self.assertEqual(self.pathU.getMimeType(mimeType), "text/plain")
            if mimeType == "pic":
                self.assertEqual(self.pathU.getMimeType(mimeType), "application/python-pickle")
            if mimeType == "other":
                self.assertEqual(self.pathU.getMimeType(mimeType), "text/plain")


def contentFormatTypeSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PathUtilsTests("testCheckContentTypeFormat"))
    suiteSelect.addTest(PathUtilsTests("testGetMimeType"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = contentFormatTypeSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
