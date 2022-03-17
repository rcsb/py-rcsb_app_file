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

import asyncio
import gzip
import logging
import os
import platform
import resource
import time
import unittest
import uuid

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))


from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter


# sl = logging.StreamHandler()
# sl.setFormatter(StructFormatter(fmt=None, mask=None))
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
# logger.addHandler(sl)
# logger.propagate = True
# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger.setLevel(logging.INFO)


class FileUploadTests(unittest.TestCase):
    def setUp(self):

        self.__dataPath = os.path.join(HERE, "test-data")
        self.__cachePath = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__sessionPath = os.path.join(self.__cachePath, "sessions")
        self.__fU = FileUtil()
        self.__fU.remove(self.__sessionPath)
        self.__fU.mkdir(self.__sessionPath)
        #
        for fn in ["example-data.cif"]:
            self.__fU.put(os.path.join(self.__dataPath, "config", fn), os.path.join(self.__cachePath, "config", fn))
        #
        nB = 25000000
        self.__testFilePath = os.path.join(self.__sessionPath, "testFile.dat")
        with open(self.__testFilePath, "wb") as ofh:
            ofh.write(os.urandom(nB))  # generate random content file
        #
        self.__testFileGzipPath = os.path.join(self.__sessionPath, "testFile.dat.gz")
        with gzip.open(self.__testFileGzipPath, "wb") as ofh:
            with open(self.__testFilePath, "rb") as ifh:
                ofh.write(ifh.read())
        #
        # Note - testConfigProvider() must precede this test to install a bootstrap configuration file
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath, self.__configFilePath).createToken({}, subject)}
        logger.debug("header %r", self.__headerD)
        # clear any previous data
        self.__repositoryPath = cP.get("REPOSITORY_DIR_PATH")
#       self.__fU.remove(self.__repositoryPath)
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
        #  Using the uncompressed hash
        hD = CryptUtils().getFileHash(self.__testFilePath, hashType=hashType)
        testHash = hD["hashDigest"]
        #
        for testFilePath, copyMode, partNumber, allowOverWrite, responseCode in [
            (self.__testFilePath, "native", 1, True, 200),
            (self.__testFilePath, "shell", 2, True, 200),
            (self.__testFilePath, "native", 1, False, 405),
            (self.__testFileGzipPath, "decompress_gzip", 3, True, 200),
        ]:
            for version in range(1, 10):
                startTime = time.time()
                try:
                    mD = {
                        "idCode": "D_00000000",
                        "repositoryType": "onedep-archive",
                        "contentType": "model",
                        "contentFormat": "pdbx",
                        "partNumber": partNumber,
                        "version": str(version),
                        "copyMode": copyMode,
                        "allowOverwrite": allowOverWrite,
                        "hashType": hashType,
                        "hashDigest": testHash,
                    }
                    #
                    with TestClient(app) as client:
                        with open(testFilePath, "rb") as ifh:
                            files = {"uploadFile": ifh}
                            response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                        if response.status_code != responseCode:
                            logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                        self.assertTrue(response.status_code == responseCode)
                        rD = response.json()
                        logger.info("rD %r", rD.items())
                        if responseCode == 200:
                            self.assertTrue(rD["success"])
                    #
                    logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s", str(e))
                    self.fail()

    def testUploadAccessTokens(self):
        """Test - upload token security (all tests should be blocked)"""
        testFilePath = self.__testFilePath
        hashType = testHash = None
        useHash = False
        if useHash:
            hashType = "MD5"
            hD = CryptUtils().getFileHash(testFilePath, hashType=hashType)
            testHash = hD["hashDigest"]

        headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath, self.__configFilePath).createToken({}, "badSubject")}
        for endPoint in ["upload"]:
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
        for endPoint in ["upload"]:
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

    def testSlicedUpload(self):
        """Test - sliced file upload operations"""
        hashType = None
        endPoint = "upload-slice"
        hashType = "MD5"
        #  Using the uncompressed hash
        hD = CryptUtils().getFileHash(self.__testFilePath, hashType=hashType)
        fullTestHash = hD["hashDigest"]
        #
        # --
        # - split the test file --
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        ioU = IoUtils(cP)
        sessionId = uuid.uuid4().hex
        # --
        sliceTotal = 4
        loop = asyncio.get_event_loop()
        task = ioU.splitFile(self.__testFilePath, sliceTotal, "staging" + sessionId, hashType="md5")
        sP = loop.run_until_complete(task)
        # loop.close()
        # --
        logger.debug("Session path %r", sP)
        #
        # --
        #
        sliceIndex = 0
        responseCode = 200
        manifestPath = os.path.join(sP, "MANIFEST")
        with open(manifestPath, "r", encoding="utf-8") as ifh:
            for line in ifh:
                testFile = line[:-1]
                testFilePath = os.path.join(sP, testFile)
                sliceIndex += 1
                startTime = time.time()
                try:
                    mD = {
                        "sliceIndex": sliceIndex,
                        "sliceTotal": sliceTotal,
                        "sessionId": sessionId,
                        "copyMode": "native",
                        "allowOverwrite": True,
                        "hashType": None,
                        "hashDigest": None,
                    }
                    #
                    with TestClient(app) as client:
                        with open(testFilePath, "rb") as ifh:
                            files = {"uploadFile": ifh}
                            response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                        if response.status_code != responseCode:
                            logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                        self.assertTrue(response.status_code == responseCode)
                        rD = response.json()
                        logger.debug("rD %r", rD.items())
                        if responseCode == 200:
                            self.assertTrue(rD["success"])
                    #
                    logger.info("Completed slice (%d) on %s (%.4f seconds)", sliceIndex, endPoint, time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s", str(e))
                    self.fail()
        #
        endPoint = "join-slice"
        startTime = time.time()
        partNumber = 1
        allowOverWrite = True
        responseCode = 200
        version = 1
        try:
            mD = {
                "sessionId": sessionId,
                "sliceTotal": sliceTotal,
                "idCode": "D_00000000",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": partNumber,
                "version": str(version),
                "copyMode": "native",
                "allowOverwrite": allowOverWrite,
                "hashType": hashType,
                "hashDigest": fullTestHash,
            }
            #
            with TestClient(app) as client:
                with open(testFilePath, "rb") as ifh:
                    response = client.post("/file-v1/%s" % endPoint, data=mD, headers=self.__headerD)
                if response.status_code != responseCode:
                    logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                self.assertTrue(response.status_code == responseCode)
                rD = response.json()
                logger.info("rD %r", rD.items())
                if responseCode == 200:
                    self.assertTrue(rD["success"])
            #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def uploadSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileUploadTests("testSimpleUpload"))
    suiteSelect.addTest(FileUploadTests("testUploadAccessTokens"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = uploadSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
