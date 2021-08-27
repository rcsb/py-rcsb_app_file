##
# File:    testFileUpload.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update:
#
#
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
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FileUploadTests(unittest.TestCase):
    def setUp(self):

        self.__dataPath = os.path.join(HERE, "test-data")
        self.__cachePath = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
        self.__sessionPath = os.path.join(self.__cachePath, "sessions")
        self.__fU = FileUtil()
        self.__fU.mkdir(self.__sessionPath)
        #
        for fn in ["example-data.cif"]:
            self.__fU.put(os.path.join(self.__dataPath, "config", fn), os.path.join(self.__cachePath, "config", fn))
        #
        nB = 25000000
        self.__testFilePath = os.path.join(self.__sessionPath, "testFile.dat")
        with open(self.__testFilePath, "wb") as ofh:
            ofh.write(os.urandom(nB))  # generate random content file
        # Note - testConfigProvider() must precede this test to install a bootstrap configuration file
        cP = ConfigProvider(self.__cachePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath).createToken({}, subject)}
        logger.info("header %r", self.__headerD)
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
        testFilePath = self.__testFilePath
        hashType = testHash = None
        useHash = False
        if useHash:
            hashType = "MD5"
            hD = CryptUtils().getFileHash(testFilePath, hashType=hashType)
            testHash = hD["hashDigest"]

        for endPoint in ["upload-shutil", "upload-aiof"]:
            startTime = time.time()
            try:
                mD = {"idCode": "D_00000000", "hashDigest": testHash, "hashType": hashType}
                #
                with TestClient(app) as client:
                    with open(testFilePath, "rb") as ifh:
                        files = {"uploadFile": ifh}
                        response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                    if response.status_code != 200:
                        logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                    self.assertTrue(response.status_code == 200)
                    rD = response.json()
                    logger.debug("rD %r", rD.items())
                    self.assertTrue(rD["success"])
                #
                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()

    def testUploadTokens(self):
        """Test - upload token security"""
        testFilePath = self.__testFilePath
        hashType = testHash = None
        useHash = False
        if useHash:
            hashType = "MD5"
            hD = CryptUtils().getFileHash(testFilePath, hashType=hashType)
            testHash = hD["hashDigest"]

        headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath).createToken({}, "badSubject")}
        for endPoint in ["upload-shutil", "upload-aiof"]:
            startTime = time.time()
            try:
                mD = {"idCode": "D_00000000", "hashDigest": testHash, "hashType": hashType}
                with TestClient(app) as client:
                    with open(testFilePath, "rb") as ifh:
                        files = {"uploadFile": ifh}
                        response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=headerD)
                    if response.status_code != 403:
                        logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                    self.assertTrue(response.status_code == 403)
                    rD = response.json()
                    logger.info("rD %r", rD.items())
                    self.assertTrue(rD["detail"] == "Invalid or expired token")
                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()
        headerD = {}
        for endPoint in ["upload-shutil", "upload-aiof"]:
            startTime = time.time()
            try:
                mD = {"idCode": "D_00000000", "hashDigest": testHash, "hashType": hashType}
                with TestClient(app) as client:
                    with open(testFilePath, "rb") as ifh:
                        files = {"uploadFile": ifh}
                        response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=headerD)
                    if response.status_code != 403:
                        logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                    self.assertTrue(response.status_code == 403)
                    rD = response.json()
                    logger.info("rD %r", rD.items())
                    self.assertTrue(rD["detail"] == "Not authenticated")
                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()


def uploadSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileUploadTests("testSimpleUpload"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = uploadSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
