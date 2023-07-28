##
# File:    testServerStatus.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update: James Smith 2023
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
import subprocess
import resource
import time
import unittest
import requests
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ServerStatusTests(unittest.TestCase):
    # comment out if running gunicorn or uvicorn
    # runs only once
    @classmethod
    def setUpClass(cls):
        subprocess.Popen(
            ["uvicorn", "rcsb.app.file.main:app"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        time.sleep(5)

    # comment out if running gunicorn or uvicorn
    # runs only once
    @classmethod
    def tearDownClass(cls):
        os.system(
            "pid=$(ps -e | grep uvicorn | head -n1 | awk '{print $1;}';);kill $pid;"
        )

    def setUp(self):
        self.__startTime = time.time()
        #
        logger.debug("Running tests on version %s", __version__)
        logger.info(
            "Starting %s at %s",
            self.id(),
            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
        )
        cP = ConfigProvider()
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {
            "Authorization": "Bearer " + JWTAuthToken().createToken({}, subject)
        }
        self.__baseUrl = "http://127.0.0.1:8000"  # cP.get("SERVER_HOST_AND_PORT")

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10**6, unitS)
        endTime = time.time()
        logger.info(
            "Completed %s at %s (%.4f seconds)",
            self.id(),
            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
            endTime - self.__startTime,
        )

    def testRootStatus(self):
        """Get root status ()."""
        try:
            url = self.__baseUrl + "/status"
            response = requests.get(url, headers=self.__headerD, timeout=None)
            self.assertTrue(response.status_code == 200)
            self.assertTrue(response.json() and len(response.json()) > 0)
            logger.info("Status %r response %r", response.status_code, response.json())
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    # def testProcessStatus(self):
    #     """Get process status ()."""
    #     try:
    #         url = self.__baseUrl + "/processStatus"
    #         response = requests.get(url, headers=self.__headerD, timeout=None)
    #         logger.info("Status %r response %r", response.status_code, response.json())
    #         self.assertTrue(response.status_code == 200)
    #     except Exception as e:
    #         logger.exception("Failing with %s", str(e))
    #         self.fail()


def apiSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ServerStatusTests("testRootStatus"))
    # suiteSelect.addTest(ServerStatusTests("testProcessStatus"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = apiSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
