##
# File:    testFileUpdate.py
# Author:  J. Westbrook
# Date:    11-Aug-2020
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for file download and upload APIs.

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


class FileUpdateTests(unittest.TestCase):
    def setUp(self):
        self.__cachePath = os.environ.get("CACHE_PATH")
        self.__configFilePath = os.environ.get("CONFIG_FILE")
#       self.__dataPath = os.path.join(HERE, "test-data")
#       self.__testFilePath = os.path.join(self.__dataPath, "config", "example-data.cif")
        self.__downloadFilePath = os.path.join(self.__cachePath, "downloadFile.cif")
        self.__updatedFilePath = os.path.join(self.__cachePath, "updatedFile.cif")
        

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

    def testSimpleUpdate(self):
        """Test - simple file download/upload"""
#       testFilePath = self.__testFilePath
        refHashType = refHashDigest = None
#       useHash = True
#       if useHash:
#           refHashType = "MD5"
#           hD = CryptUtils().getFileHash(testFilePath, hashType=refHashType)
#           refHashDigest = hD["hashDigest"]

        responseCode = 200
        for endPoint in ["download/onedep-archive"]:
            startTime = time.time()
            try:
                mD = {
                    "idCode": "D_8000210008",
                    "contentType": "model",
                    "contentFormat": "pdbx",
                    "partNumber": 1,
                    "version": "latest",
                    "hashType": refHashType,
                }
                #
                with TestClient(app) as client:
                    response = client.get("/file-v1/%s" % endPoint, params=mD, headers=self.__headerD)
                    logger.info("download response status code %r", response.status_code)
                    logger.debug("response %r %r %r", response.status_code, response.reason, response.content)
                    self.assertTrue(response.status_code == 200)
                    logger.info("Content length (%d)", len(response.content))
#                   rspHashType = response.headers["rcsb_hash_type"]
#                   rspHashDigest = response.headers["rcsb_hexdigest"]
                    with open(self.__downloadFilePath, "wb") as ofh:
                        ofh.write(response.content)
                    #
#                   thD = CryptUtils().getFileHash(self.__downloadFilePath, hashType=rspHashType)
#                   self.assertEqual(thD["hashDigest"], rspHashDigest)
#                   self.assertEqual(thD["hashDigest"], refHashDigest)
                    #
                logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
            except Exception as e:
                logger.exception("Failing with %s", str(e))
                self.fail()
            #
        #

        #update file content
        ifh = open(self.__downloadFilePath, "r")
        dataContent = ifh.read()
        ifh.close()
        ofh = open(self.__updatedFilePath, "w")
        ofh.write(dataContent.replace("PROC", "REL"))
        ofh.close()

        endPoint = "upload"
        hashType = "MD5"
        hD = CryptUtils().getFileHash(self.__updatedFilePath, hashType=hashType)
        testHash = hD["hashDigest"]

        startTime = time.time()
        try:
            mD = {
                "idCode": "D_8000210008",
                "repositoryType": "onedep-archive",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": "next",
                "copyMode": "native",
                "allowOverwrite": True,
                "hashType": hashType,
                "hashDigest": testHash,
            }
            #
            with TestClient(app) as client:
                with open(self.__updatedFilePath, "rb") as ifh:
                    files = {"uploadFile": ifh}
                    response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                #
                if response.status_code != responseCode:
                    logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                #
                self.assertTrue(response.status_code == responseCode)
                rD = response.json()
                logger.info("rD %r", rD.items())
                if responseCode == 200:
                    self.assertTrue(rD["success"])
                #
            #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()
        #

        startTime = time.time()
        try:
            mD = {
                "idCode": "D_8000210008",
                "repositoryType": "onedep-deposit",
                "contentType": "model",
                "contentFormat": "pdbx",
                "partNumber": 1,
                "version": "next",
                "copyMode": "native",
                "allowOverwrite": True,
                "hashType": hashType,
                "hashDigest": testHash,
            }
            #
            with TestClient(app) as client:
                with open(self.__updatedFilePath, "rb") as ifh:
                    files = {"uploadFile": ifh}
                    response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                #
                if response.status_code != responseCode:
                    logger.info("response %r %r %r", response.status_code, response.reason, response.content)
                #
                self.assertTrue(response.status_code == responseCode)
                rD = response.json()
                logger.info("rD %r", rD.items())
                if responseCode == 200:
                    self.assertTrue(rD["success"])
                #
            #
            logger.info("Completed %s (%.4f seconds)", endPoint, time.time() - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()
        #

def updateSimpleTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(FileUpdateTests("testSimpleUpdate"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = updateSimpleTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
