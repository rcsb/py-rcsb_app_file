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
# HERE = os.path.abspath(os.path.dirname(__file__))
# TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

import rcsb.app.config.setConfig  # noqa: F401 pylint: disable=W0611
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter
from rcsb.app.client.python.ClientUtils import ClientUtils

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
logger.setLevel(logging.INFO)


class ClientTests(unittest.TestCase):

    def setUp(self):
        self.__cU = ClientUtils(unit_test=True)
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__cP = ConfigProvider(self.__configFilePath)
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__hashType = self.__cP.get("HASH_TYPE")
        self.__dataPath = self.__cP.get("REPOSITORY_DIR_PATH")  # os.path.join(HERE, "data")
        self.__repositoryType = "unit-test"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        logger.info("self.__dataPath %s", self.__dataPath)
        logger.info("self.__configFilePath %s", self.__configFilePath)
        self.__fU = FileUtil()

        self.__repositoryFile1 = os.path.join(self.__dataPath, self.__repositoryType, "D_1000000001", "D_1000000001_model_P1.cif.V1")
        self.__repositoryFile2 = os.path.join(self.__dataPath, self.__repositoryType, "D_1000000001", "D_1000000001_model_P2.cif.V1")
        self.__repositoryFile3 = os.path.join(self.__dataPath, self.__repositoryType, "D_1000000001", "D_1000000001_model_P3.cif.V1")
        self.__downloadFile = os.path.join(self.__dataPath, "download_D_1000000001_model_P1.cif.V1")
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
            nB = self.__chunkSize * 2
            with open(self.__testFileDatPath, "wb") as out:
                out.write(os.urandom(nB))
        self.__testFileGzipPath = os.path.join(self.__dataPath, "testFile.dat.gz")
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        self.__fU.compress(self.__testFileDatPath, self.__testFileGzipPath)

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
        if os.path.exists(self.__downloadFile):
            os.unlink(self.__downloadFile)
        # warning - do not delete the data/repository folder for production, just the unit-test folder within that folder
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleUpload(self):
        """Test - basic file upload """
        resumable = False
        for testFilePath, decompress, partNumber, allowOverwrite, responseCode in [
            (self.__testFileDatPath, False, 1, True, 200),
            (self.__testFileDatPath, False, 2, True, 200),
            (self.__testFileDatPath, False, 1, False, 400),
            (self.__testFileGzipPath, True, 3, True, 200),
        ]:
            logging.warning(f"{decompress} {partNumber} {allowOverwrite} {responseCode}")
            repositoryType = self.__repositoryType
            depId = "D_1000000001"
            contentType = "model"
            milestone = ""
            contentFormat = "pdbx"
            for version in range(1, 2):
                startTime = time.time()
                try:
                    response = self.__cU.upload(testFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, decompress, allowOverwrite, resumable)
                    if not allowOverwrite:
                        self.assertTrue(response is None, "error - did not detect pre-existing file")
                    if not response:
                        logger.info("error in test simple upload")
                        break
                    self.assertTrue(response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()

    def testResumableUpload(self):
        """Test - resumable file upload """
        resumable = True
        for testFilePath, decompress, partNumber, allowOverwrite, responseCode in [
            (self.__testFileDatPath, False, 1, True, 200),
            (self.__testFileDatPath, False, 2, True, 200),
            (self.__testFileDatPath, False, 1, False, 405),
            (self.__testFileGzipPath, True, 3, True, 200),
        ]:
            logging.warning(f"{decompress} {partNumber} {allowOverwrite} {responseCode}")
            repositoryType = self.__repositoryType
            depId = "D_1000000001"
            contentType = "model"
            milestone = None
            contentFormat = "pdbx"
            for version in range(1, 2):
                startTime = time.time()
                try:
                    # get upload parameters
                    # null test - should find nothing
                    response = self.__cU.getUploadParameters(testFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite, resumable)
                    if not allowOverwrite:
                        self.assertTrue(response is None, "error - did not detect pre-existing file")
                    if not response:
                        logger.info("error in get upload parameters")
                        break
                    logger.info(f"response {response}")
                    saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash = response
                    self.assertTrue(chunkIndex == 0, f"error - chunk index {chunkIndex}")
                    # upload first chunk, not last chunk
                    for index in range(chunkIndex, expectedChunks - 1):
                        response = self.__cU.uploadChunk(testFilePath, saveFilePath, index, expectedChunks, uploadId, fullTestHash, decompress, allowOverwrite, resumable)
                        if not response:
                            logger.info("error in upload chunk")
                            break
                    logger.info(response)
                    self.assertTrue(response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    # get upload parameters - should find at least one chunk
                    response = self.__cU.getUploadParameters(testFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, allowOverwrite, resumable)
                    if not allowOverwrite:
                        self.assertTrue(response is None, "error - did not detect pre-existing file")
                    if not response:
                        logger.info("error in get upload parameters")
                        break
                    logger.info(f"response {response}")
                    saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash = response
                    self.assertTrue(chunkIndex > 0, f"error - chunk index {chunkIndex}")
                    # upload remaining chunks
                    for index in range(chunkIndex, expectedChunks):
                        response = self.__cU.uploadChunk(testFilePath, saveFilePath, index, expectedChunks, uploadId, fullTestHash, decompress, allowOverwrite, resumable)
                        if not response:
                            logger.info("error in upload chunk")
                            break
                    logger.info(response)
                    self.assertTrue(response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()

    def testSimpleDownload(self):
        """Test - basic file download """
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        for downloadFolderPath, partNumber, allowOverwrite, responseCode in [
            (self.__dataPath, 1, True, 200)
        ]:
            logging.warning(f"{partNumber} {allowOverwrite} {responseCode}")
            repositoryType = self.__repositoryType
            depId = "D_1000000001"
            contentType = "model"
            milestone = ""
            contentFormat = "pdbx"
            for version in range(1, 2):
                startTime = time.time()
                try:
                    response = self.__cU.download(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, self.__hashType, downloadFolderPath, allowOverwrite)
                    self.assertTrue(response == responseCode or response.status_code == responseCode or (response.status_code >= 400 and responseCode >= 400))
                    logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
                except Exception as e:
                    logger.exception("Failing with %s (%.4f seconds)", str(e), time.time() - startTime)
                    self.fail()

    def testListDir(self):
        """Test - list dir"""
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        try:
            # First test for dir that actually exists
            repoType = self.__repositoryType
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

    def testFilePathRemote(self):
        repoType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1
        filePathResponse = self.__cU.getFilePathRemote(repoType,depId,contentType,milestone,partNumber,contentFormat,version)
        self.assertTrue(filePathResponse == self.__repositoryFile1, f'error {filePathResponse} {self.__repositoryFile1}')

    def testFilePathLocal(self):
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
            nB = 64
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        repoType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1
        filename = self.__cU.getFilePathLocal(repoType,depId,contentType,milestone,partNumber,contentFormat,version)
        print(f'temp file path {filename}')
        self.assertTrue(os.path.exists(filename), f'error - {filename} does not exist')
        with open(filename, "rb") as r:
            bytes = r.read()
            print(bytes)
        os.unlink(filename)  # if had not made temp file with delete=false, would need file.close()
        self.assertFalse(os.path.exists(filename), f'error - {filename} exists')

    def testDirExists(self):
        repoType = self.__repositoryType
        depId = "D_1000000001"
        response = self.__cU.dir_exist(repoType, depId)
        self.assertTrue(response)

def client_tests():
    suite = unittest.TestSuite()
    suite.addTest(ClientTests("testSimpleUpload"))
    suite.addTest(ClientTests("testResumableUpload"))
    suite.addTest(ClientTests("testSimpleDownload"))
    suite.addTest(ClientTests("testListDir"))
    suite.addTest(ClientTests("testFilePathRemote"))
    suite.addTest(ClientTests("testFilePathLocal"))
    suite.addTest(ClientTests("testDirExists"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(client_tests())
