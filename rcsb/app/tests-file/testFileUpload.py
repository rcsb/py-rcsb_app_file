##
# File:    testFileUpload.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update: James Smith 2023
#
#
# Notes:
# If running manually with API running with gunicorn (outside of docker), run from ~/rcsb/py-rcsb_app_file:
#   python ./rcsb/app/tests-file/testFileUpload.py
##
"""
Tests for file upload API.

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

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
logger.setLevel(logging.INFO)


class FileUploadTests(unittest.TestCase):
    testSliceUpload = False

    def setUp(self):

        self.__dataPath = os.path.join(HERE, "data")
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        logger.info("self.__dataPath %s", self.__dataPath)
        logger.info("self.__configFilePath %s", self.__configFilePath)
        self.__fU = FileUtil()

        # Generate testFile.dat and gzipped version of file for testing gzip upload (must retain unzipped file for hash-comparison purposes)
        nB = 2500000
        self.__testFileDatPath = os.path.join(self.__dataPath, "testFile.dat")
        self.__testFileGzipPath = os.path.join(self.__dataPath, "testFile.dat.gz")
        self.__fU.compress(self.__testFileDatPath, self.__testFileGzipPath)
        #
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")  # This is needed to prepare input for testFileDownlaod to work
        #
        # Note - testConfigProvider() must precede this test to install a bootstrap configuration file
        cP = ConfigProvider(self.__configFilePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__configFilePath).createToken({}, subject)}
        logger.debug("header %r", self.__headerD)
        #
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

    def testSimpleUpload(self):
        """Test - basic file upload operations"""
        hashType = testHash = None
        endPoint = "upload"
        hashType = "MD5"
        #
        for testFilePath, copyMode, partNumber, allowOverwrite, responseCode in [
            (self.__testFilePath, "native", 1, True, 200),
            (self.__testFilePath, "shell", 2, True, 200),
            (self.__testFilePath, "native", 1, False, 405),
            (self.__testFileGzipPath, "decompress_gzip", 3, True, 200),
        ]:
            print(f'{copyMode} {partNumber} {allowOverwrite} {responseCode}')
            hD = CryptUtils().getFileHash(testFilePath, hashType=hashType)
            testHash = hD['hashDigest']
            print("testHash", testHash)
            for version in range(1, 2):
                startTime = time.time()
                try:
                    mD = {
                        "hashType": hashType,
                        "hashDigest": testHash,
                        "repositoryType": "onedep-archive",
                        "depId": "D_1000000001",
                        "contentType": "model",
                        "milestone": "",
                        "partNumber": partNumber,
                        "contentFormat": "pdbx",
                        "version": str(version),
                        "copyMode": copyMode,
                        "allowOverwrite": allowOverwrite,
                    }
                    #
                    with TestClient(app) as client:
                        with open(testFilePath, "rb") as ifh:
                            files = {"uploadFile": ifh}
                            logging.warning(f'UPLOADING {version}')
                            response = client.post("/file-v2/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                        print(f'STATUS CODE {response.status_code}')
                        self.assertTrue(response.status_code == responseCode or (response.status_code != 200 and responseCode != 200))
                    #
                    logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()


def uploadSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileUploadTests("testSimpleUpload"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = uploadSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
