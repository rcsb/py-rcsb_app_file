##
# File - testClientUtils.py
# Author - James Smith 2023
#
##

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import subprocess
import logging
import os
import platform
import resource
import time
import unittest
import shutil

# requires server

from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter
from rcsb.app.client.ClientUtils import ClientUtils
from rcsb.app.file.PathProvider import PathProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
logger.setLevel(logging.INFO)


class ClientTests(unittest.TestCase):

    # comment out if running gunicorn or uvicorn
    # runs only once
    @classmethod
    def setUpClass(cls):
        subprocess.Popen(['uvicorn', 'rcsb.app.file.main:app'], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    # comment out if running gunicorn or uvicorn
    # runs only once
    @classmethod
    def tearDownClass(cls):
        os.system("pid=$(ps -e | grep uvicorn | head -n1 | awk '{print $1;}';);kill $pid;")

    # runs before each test
    def setUp(self):
        logger.info("setting up")

        self.__cU = ClientUtils()
        self.__cP = ConfigProvider()
        self.__fU = FileUtil()

        self.__configFilePath = self.__cP.getConfigFilePath()
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__hashType = self.__cP.get("HASH_TYPE")

        self.__dataPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__repositoryType = "unit-test"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        logger.info("self.__dataPath %s", self.__unitTestFolder)

        self.__repositoryFile1 = os.path.join(self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P1.cif.V1")
        self.__repositoryFile2 = os.path.join(self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P2.cif.V1")
        self.__repositoryFile3 = os.path.join(self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P3.cif.V1")
        # os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        if not os.path.exists(self.__repositoryFile2):
            os.makedirs(os.path.dirname(self.__repositoryFile2), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFile2, "wb") as out:
                out.write(os.urandom(nB))
        if not os.path.exists(self.__repositoryFile3):
            os.makedirs(os.path.dirname(self.__repositoryFile3), mode=0o757, exist_ok=True)
            nB = self.__chunkSize
            with open(self.__repositoryFile3, "wb") as out:
                out.write(os.urandom(nB))

        self.__downloadFile = os.path.join(self.__unitTestFolder, "D_1000000001_model_P1.cif.V1")
        self.__testFileDatPath = os.path.join(self.__unitTestFolder, "testFile.dat")
        if not os.path.exists(self.__testFileDatPath):
            os.makedirs(os.path.dirname(self.__testFileDatPath), mode=0o757, exist_ok=True)
            nB = self.__chunkSize * 2
            with open(self.__testFileDatPath, "wb") as out:
                out.write(os.urandom(nB))
        self.__testFileGzipPath = os.path.join(self.__unitTestFolder, "testFile.dat.gz")
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        self.__fU.compress(self.__testFileDatPath, self.__testFileGzipPath)

        self.__startTime = time.time()

        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    # runs after each test
    def tearDown(self):
        logger.info("tearing down")
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
        logger.info("Finished at %s (%.4f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleUpload(self, resumable=False):
        logger.info("test simple upload")
        self.assertTrue(os.path.exists(self.__testFileDatPath))
        self.assertTrue(os.path.exists(self.__testFileGzipPath))

        repositoryType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = ""
        contentFormat = "pdbx"
        version = 1

        try:
            # return 200
            partNumber = 1
            decompress = False
            allowOverwrite = True
            response = self.__cU.upload(self.__testFileDatPath, repositoryType, depId, contentType, milestone, partNumber,
                                        contentFormat, version, decompress, allowOverwrite, resumable)
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}")
            self.assertTrue(response["status_code"] == 200)

            # return 200
            partNumber = 2
            response = self.__cU.upload(self.__testFileDatPath, repositoryType, depId, contentType, milestone, partNumber,
                                        contentFormat, version, decompress, allowOverwrite, resumable)
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}")
            self.assertTrue(response["status_code"] == 200)

            # return 400 (file already exists)
            partNumber = 1
            allowOverwrite = False
            response = self.__cU.upload(self.__testFileDatPath, repositoryType, depId, contentType, milestone, partNumber,
                                        contentFormat, version, decompress, allowOverwrite, resumable)
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}")
            self.assertTrue(response["status_code"] == 400)

            # return 200 (decompress gzip file)
            partNumber = 3
            decompress = True
            allowOverwrite = True
            response = self.__cU.upload(self.__testFileGzipPath, repositoryType, depId, contentType, milestone, partNumber,
                                        contentFormat, version, decompress, allowOverwrite, resumable)
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}")
            self.assertTrue(response["status_code"] == 200)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testResumableUpload(self):
        logger.info("test resumable upload")
        self.testSimpleUpload(True)

    def testSimpleDownload(self):
        logger.info("test simple download")
        self.assertTrue(os.path.exists(self.__repositoryFile1))
        self.assertTrue(os.path.exists(self.__repositoryFile2))
        repositoryType = self.__repositoryType
        downloadFolderPath = self.__unitTestFolder
        allowOverwrite = True

        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1

        try:
            response = self.__cU.download(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version,
                                      downloadFolderPath, allowOverwrite)
            logger.info(f"{PathProvider().getFileName(depId,contentType,milestone,partNumber,contentFormat,version)} 200 = {response['status_code']}")
            self.assertTrue(response["status_code"] == 200)
        except Exception as e:
            logger.info(f"exception {str(e)}")

        version = 2
        try:
            response = self.__cU.download(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version,
                                          downloadFolderPath, allowOverwrite)
            logger.info(f"{PathProvider().getFileName(depId,contentType,milestone,partNumber,contentFormat,version)} 404 = {response['status_code']}")
            self.assertTrue(response["status_code"] == 404)
        except Exception as e:
            logger.info(f"exception {str(e)}")

    def testChunkDownload(self):
        logger.info("test chunk download")
        self.assertTrue(os.path.exists(self.__repositoryFile1))
        self.assertTrue(os.path.exists(self.__repositoryFile2))
        repositoryType = self.__repositoryType
        downloadFolderPath = self.__unitTestFolder
        allowOverwrite = True

        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1

        chunkSize = self.__chunkSize
        chunkIndex = 0

        try:
            response = self.__cU.download(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version,
                                      downloadFolderPath, allowOverwrite)
            logger.info(f"{PathProvider().getFileName(depId,contentType,milestone,partNumber,contentFormat,version)} 200 = {response['status_code']}")
            self.assertTrue(response["status_code"] == 200)
            fileSize = os.path.getsize(self.__downloadFile)
            self.assertTrue(fileSize == self.__chunkSize)
        except Exception as e:
            logger.info(f"exception {str(e)}")

    def testListDir(self):
        logger.info("test list dir")
        try:
            # response 200
            repoType = self.__repositoryType
            depId = "D_1000000001"
            response = self.__cU.listDir(repoType, depId)
            status_code = response["status_code"]
            logger.info(f"{status_code}")
            self.assertTrue(status_code==200)
            dirList = response["content"]
            self.assertTrue(isinstance(dirList, list) and len(dirList) > 0)
            # response 404
            depId = "D_1234567890"
            response = self.__cU.listDir(repoType, depId)
            status_code = response["status_code"]
            logger.info(f"{status_code}")
            self.assertTrue(status_code == 404)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testFilePathLocal(self):
        logger.info("test file path local")
        self.assertTrue(os.path.exists(self.__repositoryFile1))
        repoType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        # test response 200
        version = 1
        response = self.__cU.getFilePathLocal(repoType,depId,contentType,milestone,partNumber,contentFormat,version)
        # treat as web request for simplicity
        status_code = response["status_code"]
        filename = response["content"]
        logger.info(f"file name {filename}")
        self.assertTrue(status_code == 200, f"error - 200 = {status_code} for {filename}")
        self.assertTrue(os.path.exists(filename), f"error - {filename} does not exist")
        # test response 404
        version = 2
        response = self.__cU.getFilePathLocal(repoType,depId,contentType,milestone,partNumber,contentFormat,version)
        status_code = response["status_code"]
        filename = response["content"]
        self.assertTrue(status_code == 404, f"error - 404 = {status_code} for {filename}")


    def testFilePathRemote(self):
        logger.info("test file path remote")
        repoType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1
        # test response 200
        response = self.__cU.getFilePathRemote(repoType,depId,contentType,milestone,partNumber,contentFormat,version)
        status_code = response["status_code"]
        filepath = response["content"]
        self.assertTrue(response["status_code"] == 200, f"error - 200 = {status_code} for {filepath}")
        logger.info(f"file path for version {version} = {filepath}")
        # test response 404
        version = 2
        response = self.__cU.getFilePathRemote(repoType,depId,contentType,milestone,partNumber,contentFormat,version)
        status_code = response["status_code"]
        filepath = response["content"]
        self.assertTrue(response["status_code"] == 404, f"error - 404 = {status_code} for {filepath}")
        logger.info(f"file path for version {version} = {filepath}")

    def testDirExists(self):
        logger.info("test dir exists")
        repoType = self.__repositoryType
        depId = "D_1000000001"
        response = self.__cU.dirExists(repoType, depId)
        logger.info(response)
        self.assertTrue(response["status_code"] == 200, "error - status code %s" % response["status_code"])

def client_tests():
    suite = unittest.TestSuite()
    suite.addTest(ClientTests("testSimpleUpload"))
    suite.addTest(ClientTests("testResumableUpload"))
    suite.addTest(ClientTests("testSimpleDownload"))
    suite.addTest(ClientTests("testChunkDownload"))
    suite.addTest(ClientTests("testListDir"))
    suite.addTest(ClientTests("testFilePathLocal"))
    suite.addTest(ClientTests("testFilePathRemote"))
    suite.addTest(ClientTests("testDirExists"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(client_tests())
