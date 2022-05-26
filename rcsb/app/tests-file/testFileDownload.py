##
# File:    testFileDownload.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for file download API.

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import platform
import random
import resource
import string
import time
import unittest

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

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


class FileDownloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Fixture to create download test data"""
        dataPath = os.path.join(HERE, "test-data")
        cachePath = os.environ.get("CACHE_PATH")
        sessionPath = os.path.join(cachePath, "sessions")
        repoTestPath = os.path.join(cachePath, "repository", "archive")

        fU = FileUtil()
        fU.mkdir(sessionPath)
        for fn in ["example-data.cif"]:  # Only needed for ConfigProvider init
            fU.put(os.path.join(dataPath, fn), os.path.join(cachePath, fn))
        #
        nB = 2500000
        testFilePath = os.path.join(sessionPath, "testFile.dat")
        with open(testFilePath, "w", encoding="utf-8") as ofh:
            ofh.write("".join(random.choices(string.ascii_uppercase + string.digits, k=nB)))
        #
        testFilePath = os.path.join(dataPath, "example-data.cif")
        FileDownloadTests.__repoFixture(repoTestPath, testFilePath)

    @classmethod
    def __repoFixture(cls, repoPath, testFilePath):
        ctFmtTupL = [
            ("model", "cif"),
            ("sf-convert-report", "cif"),
            ("sf-convert-report", "txt"),
            ("sf-upload-convert", "cif"),
            ("sf-upload", "cif"),
            ("sf", "cif"),
            #
            ("cc-assign-details", "pic"),
            ("cc-assign", "cif"),
            ("cc-dpstr-info", "cif"),
            ("cc-link", "cif"),
            ("correspondence-info", "cif"),
            ("format-check-report", "txt"),
            ("merge-xyz-report", "txt"),
            ("model-aux", "cif"),
            ("model-issues-report", "json"),
            ("model-upload-convert", "cif"),
            ("model-upload", "cif"),
            #
            ("structure-factor-report", "json"),
            ("tom-merge-report", "txt"),
            ("tom-upload-report", "txt"),
            ("val-data", "cif"),
            ("val-data", "xml"),
            ("val-report-full", "pdf"),
            ("val-report-slider", "png"),
            ("val-report-slider", "svg"),
            ("val-report-wwpdb-2fo-fc-edmap-coef", "cif"),
            ("val-report-wwpdb-fo-fc-edmap-coef", "cif"),
            ("val-report", "pdf"),
        ]
        # Example - D_1000258919_model_P1.cif.V1
        for idCode in ["D_1000000001", "D_1000000002"]:
            dirPath = os.path.join(repoPath, idCode)
            FileUtil().mkdir(dirPath)
            for pNo in ["P1", "P2"]:
                for contentType, fmt in ctFmtTupL[:6]:
                    for vS in ["V1", "V2"]:
                        fn = idCode + "_" + contentType + "_" + pNo + "." + fmt + "." + vS
                        pth = os.path.join(dirPath, fn)
                        FileUtil().put(testFilePath, pth)

    def setUp(self):
        self.__cachePath = os.environ.get("CACHE_PATH")
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__repoTestPath = os.path.join(self.__cachePath, "repository", "archive")
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")
        self.__downloadFilePath = os.path.join(self.__cachePath, "downloadFile.dat")

        if not os.environ.get("REPOSITORY_PATH", None):
            os.environ["REPOSITORY_PATH"] = self.__repoTestPath
        else:
            logger.info("Using REPOSITORY_PATH setting from environment %r", os.environ.get("REPOSITORY_PATH"))

        # Note - testConfigProvider() must precede this test to install a bootstrap configuration file
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

    def testSimpleDownload(self):
        """Test - simple file download"""
        testFilePath = self.__testFilePath
        refHashType = refHashDigest = None
        useHash = True

        # Checks file hash, testFilePath must match file being downloaded, or hashes will not match
        if useHash:
            refHashType = "MD5"
            hD = CryptUtils().getFileHash(testFilePath, hashType=refHashType)
            refHashDigest = hD["hashDigest"]

        for endPoint in ["download/onedep-archive"]:
            startTime = time.time()
            try:
                mD = {
                    "idCode": "D_1000000001",
                    "contentType": "model",
                    "contentFormat": "pdbx",
                    "partNumber": 1,
                    "version": 1,
                    "hashType": refHashType,
                }
                #
                with TestClient(app) as client:
                    response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                    logger.info("download response status code %r", response.status_code)
                    logger.debug("response %r %r %r", response.status_code, response.reason, response.content)
                    self.assertTrue(response.status_code == 200)
                    logger.info("Content length (%d)", len(response.content))
                    rspHashType = response.headers["rcsb_hash_type"]
                    rspHashDigest = response.headers["rcsb_hexdigest"]
                    with open(self.__downloadFilePath, "wb") as ofh:
                        ofh.write(response.content)
                    #
                    thD = CryptUtils().getFileHash(self.__downloadFilePath, hashType=rspHashType)
                    self.assertEqual(thD["hashDigest"], rspHashDigest)
                    self.assertEqual(thD["hashDigest"], refHashDigest)
                    #
                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()

    def testDownloadTokens(self):
        """Test - download token security"""
        mD = {
            "idCode": "D_1000000001",
            "contentType": "model",
            "contentFormat": "pdbx",
            "partNumber": 1,
            "version": 1,
            "hashType": "MD5",
        }
        headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath, self.__configFilePath).createToken({}, "badSubject")}
        for endPoint in ["download/onedep-archive"]:
            startTime = time.time()
            try:

                with TestClient(app) as client:
                    response = client.get("/file-v1/%s" % endPoint, params=mD, headers=headerD)
                    logger.info("download response status code %r", response.status_code)
                    self.assertTrue(response.status_code == 403)
                    rD = response.json()
                    self.assertTrue(rD["detail"] == "Invalid or expired token")
                    #
                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()

        headerD = {}
        for endPoint in ["download/onedep-archive"]:
            startTime = time.time()
            try:
                with TestClient(app) as client:
                    response = client.get("/file-v1/%s" % endPoint, params=mD, headers=headerD)
                    logger.info("download response status code %r", response.status_code)
                    self.assertTrue(response.status_code == 403)
                    rD = response.json()
                    self.assertTrue(rD["detail"] == "Not authenticated")
                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()


def downloadSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileDownloadTests("testSimpleDownload"))
    suiteSelect.addTest(FileDownloadTests("testDownloadTokens"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = downloadSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
