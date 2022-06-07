##
# File:    testAWSUpload.py
# Author:  Connor Parker
# Date:    27-May-2022
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for uploading to AWS Bucket

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
from fastapi.testclient import TestClient
from rcsb.app.file.main import app
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file import __version__

os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("rcsb", "app", "tests-file", "test-data", "data"))
os.environ["CONFIG_PATH"] = os.environ.get("CONFIG_FILE", os.path.join("rcsb", "app", "config", "config.yml"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class AWSUploadTests(unittest.TestCase):
    def setUp(self):
        self.__cachePath = os.environ.get("CACHE_PATH")
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

    def testAWSUpload(self):
        """Test - Upload file to AWS S3 Bucket"""

        testFileName = os.path.join(self.__cachePath, "testFile.dat")

        multiD = {
            "idCode": "D_000",
            "repositoryType": "onedep-archive",
            "contentType": "model",
            "contentFormat": "pdbx",
            "partNumber": 1,
            "version": 1
        }

        nB = 1000000
        with open(testFileName, "wb") as ofh:
            ofh.write(os.urandom(nB))
        print("file written")

        with TestClient(app) as client:
            with open(testFileName, "rb") as f:
                files = {"uploadFile": f}
                r = client.post("http://128.6.159.177:8000/file-v1/upload-aioboto3", files=files, data=multiD)
        print(r.status_code)


def updateSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(AWSUploadTests("testAWSUpload"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = updateSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
