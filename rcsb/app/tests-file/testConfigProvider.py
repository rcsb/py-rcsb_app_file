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
import platform
import resource
import time
import unittest
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file import __version__


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ConfigProviderTests(unittest.TestCase):
    def setUp(self):
        self.__startTime = time.time()
        cP = ConfigProvider()
        self.__configFilePath = cP.getConfigFilePath()
        #
        self.__cD = {}
        if self.__configFilePath:
            try:
                self.__cD = cP.getConfig()
            except Exception as e:
                logger.info("Unable to getConfig with exception %s", str(e))
                self.fail()
            #
        #
        if not self.__cD:
            raise Exception("Could not make config dictionary")

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

    def testConfigAccessors(self):
        """Test -configuration accessors"""
        cP = ConfigProvider()
        for ky, vl in self.__cD.items():
            tv = cP.get(ky)
            self.assertEqual(tv, vl)

        cP = ConfigProvider()
        for ky, vl in self.__cD.items():
            tv = cP.get(ky)
            self.assertEqual(tv, vl)

    def testValidate(self):
        """Test validation of settings"""
        cP = ConfigProvider()
        self.assertTrue(cP.validate(), "error - native config file did not validate")

        def test(key, val, expected, err):
            orig = cP.get(key)
            cP._set(key, val)
            if expected:
                self.assertTrue(cP.validate(), err)
            else:
                self.assertFalse(cP.validate(), err)
            cP._set(key, orig)

        # validate host and port (requires scheme)
        test(
            "SERVER_HOST_AND_PORT",
            "1.2.3.4:8000",
            False,
            "error - could not invalidate host and port",
        )
        test(
            "SERVER_HOST_AND_PORT",
            "https://127.0.0.1:80",
            True,
            "error - could not validate host and port",
        )
        # allow zero surplus processors (reserve all processors)
        # zero value is also falsy so ensure that validate function is able to differentiate zero from negative values
        test(
            "SURPLUS_PROCESSORS",
            1,
            True,
            "error - could not validate surplus processors"
        )
        test(
            "SURPLUS_PROCESSORS",
            0,
            True,
            "error - zero valued surplus did not validate",
        )
        test(
            "SURPLUS_PROCESSORS",
            -1,
            False,
            "error - could not invalidate surplus processors",
        )
        # test file path regex
        test(
            "REPOSITORY_DIR_PATH",
            "|path|to|file",
            False,
            "error - could not invalidate file paths",
        )
        test(
            "REPOSITORY_DIR_PATH",
            "../path/to/file/",
            True,
            "error - could not validate file paths",
        )
        test(
            "REPOSITORY_DIR_PATH",
            "local",
            True,
            "error - could not validate file paths",
        )
        test(
            "REPOSITORY_DIR_PATH",
            "/root/public_files/local/",
            True,
            "error - could not validate folder name with delimiting underscores"
        )
        test(
            "REPOSITORY_DIR_PATH",
            "/root/public-files/local/",
            True,
            "error - could not validate folder name with delimiting dashes"
        )
        test(
            "REPOSITORY_DIR_PATH",
            "/root/public files/local/",
            True,
            "error - could not validate folder with delimiting spaces"
        )
        test(
            "KV_FILE_PATH",
            "./kv.sqlite",
            True,
            "error - could not validate path with delimiting dots"
        )
        # test booleans
        test("LOCK_TRANSACTIONS", "true", False, "error - could not invalidate boolean")
        # test lock timeout and ensure falsy zero value is allowed
        test("LOCK_TIMEOUT", 1, True, "error - could not validate lock timeout with 1")
        test("LOCK_TIMEOUT", 0, True, "error - could not validate lock timeout with 0")
        test("LOCK_TIMEOUT", "", False, "error - could not invalidate lock timeout")
        test("LOCK_TIMEOUT", -1, False, "error - could not invalidate lock timeout")
        # test lock type
        test("LOCK_TYPE", "ternary", True, "error - could not validate lock type")
        test("LOCK_TYPE", 3, False, "error - could not invalidate lock type")
        # test null
        test("LOCK_TYPE", None, False, "error - could not invalidate null")
        # test empty string
        test("LOCK_TYPE", "", False, "error - could not invalidate empty string")
        # test kv mode
        test("KV_MODE", "mongo", False, "error - could not invalidate kv mode")
        # test relation between lock type and kv mode
        cP._set("LOCK_TYPE", "redis")
        cP._set("KV_MODE", "redis")
        self.assertTrue(
            cP.validate(), "error - could not validate lock type and kv mode redis"
        )
        cP._set("KV_MODE", "sqlite")
        self.assertFalse(
            cP.validate(),
            "error - could not invalidate incongruence between lock type and kv mode",
        )
        cP._set("KV_MODE", "redis")
        # test redis host
        test("REDIS_HOST", "mongo", False, "error - could not invalidate redis host")
        test(
            "REDIS_HOST",
            "http://127.0.0.1:6379",
            True,
            "error - could not validate redis host with ip address",
        )
        # validate table name
        test(
            "KV_SESSION_TABLE_NAME",
            "mongo",
            True,
            "error - could not validate string table name",
        )
        # validate max seconds
        test("KV_MAX_SECONDS", -1, False, "error - could not invalidate max seconds")
        # validate chunk size
        test("CHUNK_SIZE", -1, False, "error - could not invalidate chunk size")
        # validate compression type
        test(
            "COMPRESSION_TYPE",
            "brotli",
            False,
            "error - could not invalidate compression type",
        )
        # validate hash type
        test("HASH_TYPE", "SHA", False, "error - could not invalidate hash type")
        # validate default file permissions
        test(
            "DEFAULT_FILE_PERMISSIONS",
            "0o555",
            True,
            "error - could not validate file permissions",
        )
        test(
            "DEFAULT_FILE_PERMISSIONS",
            0,
            False,
            "error - could not invalidate file permissions",
        )
        # validate jwt algorithm
        test(
            "JWT_ALGORITHM", "SHA1", False, "error - could not invalidate jwt algorithm"
        )
        # validate jwt duration
        test("JWT_DURATION", -1, False, "error - could not invalidate jwt duration")
        # validate bypass authorization
        test(
            "BYPASS_AUTHORIZATION",
            "true",
            False,
            "error - could not invalidate bypass authorization",
        )


def configAccessorsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ConfigProviderTests("testConfigAccessors"))
    suiteSelect.addTest(ConfigProviderTests("testValidate"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = configAccessorsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
