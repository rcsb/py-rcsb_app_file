##
# File:    testFileUpload.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update:
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
        self.__cachePath = os.environ.get("CACHE_PATH")
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        logger.info("self.__dataPath %s", self.__dataPath)
        logger.info("self.__cachePath %s", self.__cachePath)
        logger.info("self.__configFilePath %s", self.__configFilePath)
        self.__sessionPath = os.path.join(self.__cachePath, "sessions")
        self.__fU = FileUtil()
        self.__fU.remove(self.__sessionPath)
        self.__fU.mkdir(self.__sessionPath)
        #
        for fn in ["example-data.cif"]:  # Only needed for ConfigProvider init
            self.__fU.put(os.path.join(self.__dataPath, fn), os.path.join(self.__cachePath, fn))
        #
        # Generate testFile.dat and gzipped version of file for testing gzip upload (must retain unzipped file for hash-comparison purposes) 
        nB = 25000000
        self.__testFileDatPath = os.path.join(self.__sessionPath, "testFile.dat")
        with open(self.__testFileDatPath, "wb") as ofh:
            ofh.write(os.urandom(nB))  # generate random content file
        #
        self.__testFileGzipPath = os.path.join(self.__sessionPath, "testFile.dat.gz")
        self.__fU.compress(self.__testFileDatPath, self.__testFileGzipPath)
        #
        # self.__testFilePath = "/Users/dennis/Desktop/emd_13856.map.gz"
        #
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")  # This is needed to prepare input for testFileDownlaod to work
        #
        # Note - testConfigProvider() must precede this test to install a bootstrap configuration file
        cP = ConfigProvider(self.__cachePath, self.__configFilePath)
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath, self.__configFilePath).createToken({}, subject)}
        # self.__headerD['Authorization'] = 'Bearer eyJ0fXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2NTI5Njc5OTYsImlhdCI6MTY1Mjk2NzA5Niwic3ViIjoiYVRlc3RTdWJqZWN0In0.gWllKHP-2YkTHUnTNQRTMPmKTxhICLjJRdK5ChZmNCU'
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
        hashType = testHash = None
        endPoint = "upload"
        hashType = "MD5"
        #
        for testFilePath, copyMode, partNumber, allowOverWrite, responseCode in [
            (self.__testFilePath, "native", 1, True, 200),
            (self.__testFilePath, "shell", 2, True, 200),
            (self.__testFilePath, "native", 1, False, 405),
            (self.__testFileGzipPath, "decompress_gzip", 3, True, 200),
        ]:
            #  Using the uncompressed hash
            if copyMode == "decompress_gzip":
                hD = CryptUtils().getFileHash(testFilePath.split(".gz")[0], hashType=hashType)
            else:
                hD = CryptUtils().getFileHash(testFilePath, hashType=hashType)
            testHash = hD["hashDigest"]
            print("testHash", testHash)
            for version in range(1, 10):
                startTime = time.time()
                try:
                    mD = {
                        "idCode": "D_1000000001",
                        "repositoryType": "onedep-archive",
                        "contentType": "model",
                        "contentFormat": "pdbx",
                        "partNumber": partNumber,
                        "version": str(version),
                        "copyMode": copyMode,
                        "allowOverWrite": allowOverWrite,
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
                            print("\n RESPONSE:", response.status_code, response.reason, response.content)
                        self.assertTrue(response.status_code == responseCode)
                        rD = response.json()
                        logger.info("rD %r", rD.items())
                        if responseCode == 200:
                            self.assertTrue(rD["success"])
                    #
                    logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
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
                mD = {"idCode": "D_1000000001", "hashDigest": testHash, "hashType": hashType}
                with TestClient(app) as client:
                    with open(testFilePath, "rb") as ifh:
                        # t1=time.time()
                        # print("opening file for handling")
                        files = {"uploadFile": ifh}
                        # print("done opening file for handling", time.time()-t1)
                        response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=headerD)
                        # print("response type", type(response))
                        # print("received response", time.time()-t1)
                        # print("\tresponse.content:", response.content)
                    if response.status_code != 403:
                        logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                    self.assertTrue(response.status_code == 403)
                    # print("dir(response)", dir(response))
                    # print("raw response", response.text)  # Should be:  {"detail":"Invalid or expired token"}
                    # print("response conent", response.content)
                    rD = response.json()
                    logger.info("rD %r", rD.items())
                    self.assertTrue(rD["detail"] == "Invalid or expired token")
                logger.info("Completed fail test for %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()
        headerD = {}
        for endPoint in ["upload"]:
            startTime = time.time()
            try:
                # print("\nON SECOND TEST")
                mD = {"idCode": "D_1000000001", "hashDigest": testHash, "hashType": hashType}
                with TestClient(app) as client:
                    with open(testFilePath, "rb") as ifh:
                        files = {"uploadFile": ifh}
                        # print("HERE IN SECOND")
                        response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=headerD)
                        # print("\n RESPONSE:", response.status_code, response.reason, response.content)
                        # print("raw response", response.text)  # Should be:  {"detail":"Invalid or expired token"}
                        # print("response conent", response.content)
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

        # First, split the file into 4 slices in a new "sessions" directory (prefixed with "staging", e.g., "stagingX1Y2Z...");
        # this also creates a "MANIFEST" file containing the names of the file slices.
        task = ioU.splitFile(self.__testFilePath, sliceTotal, "staging" + sessionId, hashType="md5")

        sP = loop.run_until_complete(task)
        # loop.close()
        # --
        logger.info("Session path %r", sP)
        #
        # --
        #
        sliceIndex = 0
        responseCode = 200
        manifestPath = os.path.join(sP, "MANIFEST")

        # Second, read the MANIFEST file to determine what slices there are, and upload each slice using endpoint "upload-slice" to a non-staging "sessions" directory
        # (e.g., if file was split into directory "sessions/stagingX1Y2Z...", the upload will be placed in adjacent directory "sessions/X1Y2Z...")
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
                        "allowOverWrite": True,
                        "hashType": None,
                        "hashDigest": None,
                    }
                    #
                    with TestClient(app) as client:
                        with open(testFilePath, "rb") as itfh:
                            files = {"uploadFile": itfh}
                            response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                        if response.status_code != responseCode:
                            logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                            # print("\n RESPONSE:", response.status_code, response.reason, response.content)
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
        # Last, join the slices in the sessions directory together into a single file in the "repository/archive/<idCode>" directory
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
                "idCode": "D_1000000001",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": partNumber,
                "version": str(version),
                "copyMode": "native",
                "allowOverWrite": allowOverWrite,
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
    # suiteSelect.addTest(FileUploadTests("testSlicedUpload"))
    # suiteSelect.addTest(FileUploadTests("testUploadAccessTokens"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = uploadSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
