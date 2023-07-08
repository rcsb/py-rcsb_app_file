##
# File:    testJWTAuthToken.py
# Author:  J. Westbrook
# Date:    24-Aug-2020
# Version: 0.001
#
# Update: James Smith 2023
#
#
##
"""
Tests for token minting and validation.

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import datetime
import logging
import platform
import resource
import time
import unittest
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file import __version__


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class JTWAuthTokenTests(unittest.TestCase):
    def setUp(self):
        self.__startTime = time.time()
        self.__subject = "aTestSubject"
        logger.debug("Running tests on version %s", __version__)
        logger.info(
            "Starting %s at %s",
            self.id(),
            time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
        )

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

    def testJwtToken(self):
        """Test - minting and verifying JWT tokens"""
        extraD = {"one": "onevalue", "two": 3, "four": 4.0}
        deltaSeconds = 3600

        au = JWTAuthToken()
        delta = datetime.timedelta(seconds=deltaSeconds)
        token = au.createToken(extraD, self.__subject, expiresDelta=delta)
        logger.info("Token %r", token)
        payload = au.decodeToken(token)
        logger.info("payload %r", payload)
        for ky, val in extraD.items():
            self.assertEqual(payload[ky], val)
        logger.info("delta (%d)", payload["exp"] - payload["iat"])
        self.assertEqual(payload["exp"] - payload["iat"], deltaSeconds)
        #


def tokenTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(JTWAuthTokenTests("testJwtToken"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = tokenTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
