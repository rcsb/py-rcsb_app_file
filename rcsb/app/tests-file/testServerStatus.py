##
# File:    testServerStatus.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for server status API requests
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import platform
import pprint
import resource
import time
import unittest

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CACHE_PATH"] = os.path.join(HERE, "test-output", "CACHE")

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.main import app

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ServerStatusTests(unittest.TestCase):
    def setUp(self):
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

    def testRootStatus(self):
        """Get root status ()."""
        try:
            with TestClient(app) as client:
                response = client.get("/")
                logger.info("Status %r response %r", response.status_code, response.json())
                self.assertTrue(response.status_code == 200)
                self.assertTrue(response.json() == {"msg": "Service is up!"})
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testProcessStatus(self):
        """Get process status ()."""
        try:
            with TestClient(app) as client:
                response = client.get("/status")
                logger.debug("Status %r response %r", response.status_code, response.json())
                self.assertTrue(response.status_code == 200)
                rD = response.json()
                self.assertGreaterEqual(rD["version"], 0.3)
                # self.assertTrue(response.json() == {"msg": "Service is up!"})
                logger.debug("Process status: %s", pprint.pformat(rD, indent=3))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testHealthCheck(self):
        """Get health check."""
        try:
            with TestClient(app) as client:
                response = client.get("/healthcheck")
                logger.info("Status %r response %r", response.status_code, response)
                self.assertTrue(response.status_code == 200)
                logger.info("Text %r", response.text.strip('"'))
                self.assertEqual(response.text.strip('"'), "UP")
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def apiSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ServerStatusTests("testRootStatus"))
    suiteSelect.addTest(ServerStatusTests("testHealthCheck"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = apiSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
