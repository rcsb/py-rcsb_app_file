##
# Author:  James Smith 2023
#
#
##

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest
import shutil

# requires server

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
from rcsb.app.file.ClientUtils import ClientUtils


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
logger.setLevel(logging.INFO)


class ClientTests(unittest.TestCase):

    def setUp(self):
        self.__cU = ClientUtils()
        self.__dataPath = os.path.join(HERE, "data")
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        logger.info("self.__dataPath %s", self.__dataPath)
        logger.info("self.__configFilePath %s", self.__configFilePath)
        self.__fU = FileUtil()

        self.__repositoryFile1 = os.path.join(self.__dataPath, "repository", "archive", "D_1000000001", "D_1000000001_model_P1.cif.V1")
        self.__repositoryFile2 = os.path.join(self.__dataPath, "repository", "archive", "D_1000000001", "D_1000000001_model_P2.cif.V1")
        self.__repositoryFile3 = os.path.join(self.__dataPath, "repository", "archive", "D_1000000001", "D_1000000001_model_P3.cif.V1")
        if os.path.exists(self.__repositoryFile1):
            os.unlink(self.__repositoryFile1)
        if os.path.exists(self.__repositoryFile2):
            os.unlink(self.__repositoryFile2)
        if os.path.exists(self.__repositoryFile3):
            os.unlink(self.__repositoryFile3)
        os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)

        self.__testFileDatPath = os.path.join(self.__dataPath, "testFile.dat")
        if not os.path.exists(self.__testFileDatPath):
            os.makedirs(os.path.dirname(self.__testFileDatPath), mode=0o757, exist_ok=True)
            nB = 1024 * 1024 * 8
            with open(self.__testFileDatPath, "wb") as out:
                out.write(os.urandom(nB))
        self.__testFileGzipPath = os.path.join(self.__dataPath, "testFile.dat.gz")
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        self.__fU.compress(self.__testFileDatPath, self.__testFileGzipPath)

        cP = ConfigProvider(self.__configFilePath)
        self.__startTime = time.time()

        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if os.path.exists(self.__repositoryFile1):
            os.unlink(self.__repositoryFile1)
        if os.path.exists(self.__repositoryFile2):
            os.unlink(self.__repositoryFile2)
        if os.path.exists(self.__repositoryFile3):
            os.unlink(self.__repositoryFile3)
        if os.path.exists(self.__testFileDatPath):
            os.unlink(self.__testFileDatPath)
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        if os.path.exists(self.__dataPath):
            shutil.rmtree(self.__dataPath)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def test_simple_upload(self):
        """Test - basic file upload """
        hashType = testHash = None
        hashType = "MD5"
        resumable = False
        for testFilePath, decompress, partNumber, allowOverwrite, responseCode in [
            (self.__testFileDatPath, False, 1, True, 200),
            (self.__testFileDatPath, False, 2, True, 200),
            (self.__testFileDatPath, False, 1, False, 400),
            (self.__testFileGzipPath, True, 3, True, 200),
        ]:
            logging.warning(f'{decompress} {partNumber} {allowOverwrite} {responseCode}')
            repositoryType = "onedep-archive"
            depId = "D_1000000001"
            contentType = "model"
            milestone = ""
            contentFormat = "pdbx"
            for version in range(1, 2):
                startTime = time.time()
                try:
                    response = self.__cU.upload(testFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, decompress, allowOverwrite, resumable)
                    self.assertTrue(response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()

    def test_resumable_upload(self):
        """Test - resumable file upload """
        hashType = testHash = None
        hashType = "MD5"
        resumable = True
        for testFilePath, decompress, partNumber, allowOverwrite, responseCode in [
            (self.__testFileDatPath, False, 1, True, 200),
            (self.__testFileDatPath, False, 2, True, 200),
            (self.__testFileDatPath, False, 1, False, 405),
            (self.__testFileGzipPath, True, 3, True, 200),
        ]:
            logging.warning(f'{decompress} {partNumber} {allowOverwrite} {responseCode}')
            repositoryType = "onedep-archive"
            depId = "D_1000000001"
            contentType = "model"
            milestone = ""
            contentFormat = "pdbx"
            for version in range(1, 2):
                startTime = time.time()
                try:
                    response = self.__cU.upload(testFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, decompress, allowOverwrite, resumable)
                    print(response)
                    self.assertTrue(response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()


    def test_simple_download(self):
        """Test - basic file download """
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
            nB = 1024 * 1024 * 8
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        hashType = testHash = None
        hashType = "MD5"
        for downloadFolderPath, partNumber, allowOverwrite, responseCode in [
            (self.__dataPath, 1, True, 200)
        ]:
            logging.warning(f'{partNumber} {allowOverwrite} {responseCode}')
            repositoryType = "onedep-archive"
            depId = "D_1000000001"
            contentType = "model"
            milestone = ""
            contentFormat = "pdbx"
            for version in range(1, 2):
                startTime = time.time()
                try:
                    response = self.__cU.download(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, hashType, downloadFolderPath, allowOverwrite)
                    self.assertTrue(response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()


    def test_list_dir(self):
        """Test - list dir"""
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
            nB = 1024 * 1024 * 8
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        try:
            # First test for dir that actually exists
            repoType = "onedep-archive"
            depId = "D_1000000001"
            response = self.__cU.listDir(repoType, depId)
            self.assertTrue(response and len(response) > 0)
            # Next test for dir that DOESN'T exists
            depId = "D_1234567890"
            response = self.__cU.listDir(repoType, depId)
            self.assertTrue(response is None or len(response) == 0)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


if __name__ == "__main__":
    unittest.main()
