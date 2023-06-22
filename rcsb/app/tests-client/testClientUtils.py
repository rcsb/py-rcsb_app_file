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
import time
import resource
import unittest
import shutil
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter
from rcsb.app.client.ClientUtils import ClientUtils
from rcsb.app.file.PathProvider import PathProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
logger.setLevel(logging.INFO)

# requires server

class ClientTests(unittest.TestCase):
    # comment out if running gunicorn or uvicorn
    # runs only once
    @classmethod
    def setUpClass(cls):
        subprocess.Popen(
            ["uvicorn", "rcsb.app.file.main:app"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        time.sleep(5)

    # comment out if running gunicorn or uvicorn
    # runs only once
    @classmethod
    def tearDownClass(cls):
        os.system(
            "pid=$(ps -e | grep uvicorn | head -n1 | awk '{print $1;}';);kill $pid;"
        )

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
        self.__repositoryType2 = "test"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        self.__unitTestFolder2 = os.path.join(self.__dataPath, self.__repositoryType2)
        logger.info("self.__dataPath %s", self.__unitTestFolder)

        self.__repositoryFile1 = os.path.join(
            self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P1.cif.V1"
        )
        self.__repositoryFile2 = os.path.join(
            self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P2.cif.V1"
        )
        self.__repositoryFile3 = os.path.join(
            self.__unitTestFolder, "D_1000000001", "D_1000000001_model_P3.cif.V1"
        )
        self.__repositoryFile4 = os.path.join(
            self.__unitTestFolder2, "D_1000000001", "D_1000000001_model_P1.cif.V1"
        )
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(
                os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True
            )
            nB = self.__chunkSize
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        if not os.path.exists(self.__repositoryFile2):
            os.makedirs(
                os.path.dirname(self.__repositoryFile2), mode=0o757, exist_ok=True
            )
            nB = self.__chunkSize
            with open(self.__repositoryFile2, "wb") as out:
                out.write(os.urandom(nB))
        if not os.path.exists(self.__repositoryFile3):
            os.makedirs(
                os.path.dirname(self.__repositoryFile3), mode=0o757, exist_ok=True
            )
            nB = self.__chunkSize
            with open(self.__repositoryFile3, "wb") as out:
                out.write(os.urandom(nB))

        self.__downloadFile = os.path.join(
            self.__unitTestFolder, "D_1000000001_model_P1.cif.V1"
        )
        self.__testFileDatPath = os.path.join(self.__unitTestFolder, "testFile.dat")
        if not os.path.exists(self.__testFileDatPath):
            os.makedirs(
                os.path.dirname(self.__testFileDatPath), mode=0o757, exist_ok=True
            )
            nB = self.__chunkSize * 2
            with open(self.__testFileDatPath, "wb") as out:
                out.write(os.urandom(nB))
        self.__testFileGzipPath = os.path.join(self.__unitTestFolder, "testFile.dat.gz")
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        self.__fU.compress(self.__testFileDatPath, self.__testFileGzipPath)

    # runs after each test
    def tearDown(self):
        logger.info("tearing down")
        if os.path.exists(self.__repositoryFile1):
            os.unlink(self.__repositoryFile1)
        if os.path.exists(self.__repositoryFile2):
            os.unlink(self.__repositoryFile2)
        if os.path.exists(self.__repositoryFile3):
            os.unlink(self.__repositoryFile3)
        if os.path.exists(self.__repositoryFile4):
            os.unlink(self.__repositoryFile4)
        if os.path.exists(self.__testFileDatPath):
            os.unlink(self.__testFileDatPath)
        if os.path.exists(self.__testFileGzipPath):
            os.unlink(self.__testFileGzipPath)
        if os.path.exists(self.__downloadFile):
            os.unlink(self.__downloadFile)
        # warning - do not delete the data/repository folder for production, just the unit-test (or test) folder within that folder
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        if os.path.exists(self.__unitTestFolder2):
            shutil.rmtree(self.__unitTestFolder2)
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10**6, unitS)

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
        fileExtension = None

        try:
            # return 200
            partNumber = 1
            decompress = False
            allowOverwrite = True
            response = self.__cU.upload(
                self.__testFileDatPath,
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                decompress,
                fileExtension,
                allowOverwrite,
                resumable
            )
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}"
            )
            self.assertTrue(response["status_code"] == 200)

            # return 200
            partNumber = 2
            response = self.__cU.upload(
                self.__testFileDatPath,
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                decompress,
                fileExtension,
                allowOverwrite,
                resumable
            )
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}"
            )
            self.assertTrue(response["status_code"] == 200)

            # return 403 (file already exists)
            partNumber = 1
            allowOverwrite = False
            response = self.__cU.upload(
                self.__testFileDatPath,
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                decompress,
                fileExtension,
                allowOverwrite,
                resumable
            )
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}"
            )
            self.assertTrue(response["status_code"] == 403)

            # return 200 (decompress gzip file)
            partNumber = 3
            decompress = True
            fileExtension = ".gz"
            allowOverwrite = True
            response = self.__cU.upload(
                self.__testFileGzipPath,
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                decompress,
                fileExtension,
                allowOverwrite,
                resumable
            )
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}"
            )
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
        # test response 200
        try:
            response = self.__cU.download(
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                downloadFolderPath,
                allowOverwrite,
            )
            logger.info(
                f"{PathProvider().getFileName(depId,contentType,milestone,partNumber,contentFormat,version)} 200 = {response['status_code']}"
            )
            self.assertTrue(response["status_code"] == 200)
        except Exception as e:
            logger.info(f"exception {str(e)}")
            self.fail()
        # test response 404
        version = 2
        try:
            response = self.__cU.download(
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                downloadFolderPath,
                allowOverwrite,
            )
            logger.info(
                f"{PathProvider().getFileName(depId,contentType,milestone,partNumber,contentFormat,version)} 404 = {response['status_code']}"
            )
            self.assertTrue(response["status_code"] == 404, "error - status code %d" % response["status_code"])
        except Exception as e:
            logger.info(f"exception {str(e)}")
            self.fail()

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

        chunkSize = self.__chunkSize // 2
        chunkIndex = 0

        response = self.__cU.download(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            downloadFolderPath,
            allowOverwrite,
            chunkSize,
            chunkIndex
        )
        logger.info(
            f"{PathProvider().getFileName(depId,contentType,milestone,partNumber,contentFormat,version)} 200 = {response['status_code']}"
        )
        self.assertTrue(response["status_code"] == 200, "error - status code %d" % response["status_code"])
        fileSize = os.path.getsize(self.__downloadFile)
        self.assertTrue(fileSize == chunkSize, "error - file size %d chunk size %d" % (fileSize, chunkSize))

    def testListDir(self):
        logger.info("test list dir")
        try:
            # response 200
            repoType = self.__repositoryType
            depId = "D_1000000001"
            response = self.__cU.listDir(repoType, depId)
            status_code = response["status_code"]
            logger.info(f"{status_code}")
            self.assertTrue(status_code == 200)
            dirList = response["dirList"]
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
        response = self.__cU.getFilePathLocal(
            repoType, depId, contentType, milestone, partNumber, contentFormat, version
        )
        # treat as web request for simplicity
        status_code = response["status_code"]
        filepath = response["filePath"]
        logger.info(f"file name {filepath}")
        self.assertTrue(
            status_code == 200, f"error - 200 = {status_code} for {filepath}"
        )
        self.assertTrue(os.path.exists(filepath), f"error - {filepath} does not exist")
        # test response 404
        version = 2
        response = self.__cU.getFilePathLocal(
            repoType, depId, contentType, milestone, partNumber, contentFormat, version
        )
        status_code = response["status_code"]
        self.assertTrue(status_code == 404, f"error - 404 = {status_code}")

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
        response = self.__cU.getFilePathRemote(
            repoType, depId, contentType, milestone, partNumber, contentFormat, version
        )
        status_code = response["status_code"]
        filepath = response["filePath"]
        self.assertTrue(
            response["status_code"] == 200,
            f"error - 200 = {status_code} for {filepath}",
        )
        logger.info(f"file path for version {version} = {filepath}")
        # test response 404
        version = 2
        response = self.__cU.getFilePathRemote(
            repoType, depId, contentType, milestone, partNumber, contentFormat, version
        )
        status_code = response["status_code"]
        self.assertTrue(response["status_code"] == 404, f"error - 404 = {status_code}")

    def testDirExists(self):
        logger.info("test dir exists")
        repoType = self.__repositoryType
        depId = "D_1000000001"
        response = self.__cU.dirExists(repoType, depId)
        logger.info(response)
        self.assertTrue(
            response["status_code"] == 200,
            "error - status code %s" % response["status_code"],
        )

    def testCopyFile(self):
        try:
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                "contentTypeSource": "model",
                "milestoneSource": "",
                "partNumberSource": 1,
                "contentFormatSource": "pdbx",
                "versionSource": 1,
                #
                "repositoryTypeTarget": self.__repositoryType2,
                "depIdTarget": "D_1000000001",
                "contentTypeTarget": "model",
                "milestoneTarget": "",
                "partNumberTarget": 1,
                "contentFormatTarget": "pdbx",
                "versionTarget": 1,
                #
                "overwrite": False,
            }
            response = self.__cU.copyFile(**mD)
            logger.info("file status %r", response["status_code"])
            self.assertTrue(
                response["status_code"] == 200,
                f'error - status code {response["status_code"]}',
            )
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(
                        PathProvider().getVersionedPath(
                            self.__repositoryType2,
                            "D_1000000001",
                            "model",
                            None,
                            1,
                            "pdbx",
                            1,
                        )
                    )
                ),
                "error - copied file not found",
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCopyDir(self):
        try:
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                #
                "repositoryTypeTarget": self.__repositoryType2,
                "depIdTarget": "D_1000000001",
                #
                "overwrite": False,
            }
            response = self.__cU.copyDir(**mD)
            logger.info("file status %r", response["status_code"])
            self.assertTrue(
                response["status_code"] == 200,
                f"error - status code {response['status_code']}",
            )
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(
                        PathProvider().getVersionedPath(
                            self.__repositoryType2,
                            "D_1000000001",
                            "model",
                            None,
                            1,
                            "pdbx",
                            1,
                        )
                    )
                ),
                "error - copied file not found",
            )
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(
                        PathProvider().getVersionedPath(
                            self.__repositoryType2,
                            "D_1000000001",
                            "model",
                            None,
                            2,
                            "pdbx",
                            1,
                        )
                    )
                ),
                "error - copied file not found",
            )
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(
                        PathProvider().getVersionedPath(
                            self.__repositoryType2,
                            "D_1000000001",
                            "model",
                            None,
                            3,
                            "pdbx",
                            1,
                        )
                    )
                ),
                "error - copied file not found",
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testMoveFile(self):
        try:
            # Move file from one repositoryType to another
            mD = {
                "repositoryTypeSource": self.__repositoryType,
                "depIdSource": "D_1000000001",
                "contentTypeSource": "model",
                "milestoneSource": "",
                "partNumberSource": 1,
                "contentFormatSource": "pdbx",
                "versionSource": 1,
                #
                "repositoryTypeTarget": self.__repositoryType2,
                "depIdTarget": "D_1000000001",
                "contentTypeTarget": "model",
                "milestoneTarget": "",
                "partNumberTarget": 1,
                "contentFormatTarget": "pdbx",
                "versionTarget": 1,
                #
                "overwrite": "False",
            }
            response = self.__cU.moveFile(**mD)
            logger.info("file status %r", response["status_code"])
            self.assertTrue(
                response["status_code"] == 200,
                f"error - status code {response['status_code']}",
            )
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(
                        PathProvider().getVersionedPath(
                            self.__repositoryType2,
                            "D_1000000001",
                            "model",
                            None,
                            1,
                            "pdbx",
                            1,
                        )
                    )
                ),
                "error - moved file not found",
            )
            self.assertFalse(
                os.path.exists(
                    os.path.abspath(
                        PathProvider().getVersionedPath(
                            self.__repositoryType,
                            "D_1000000001",
                            "model",
                            None,
                            1,
                            "pdbx",
                            1,
                        )
                    )
                ),
                "error - file was not moved",
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCompressDir(self):
        try:
            mD = {"repositoryType": self.__repositoryType, "depId": "D_1000000001"}
            response = self.__cU.compressDir(**mD)
            logger.info("file status %r", response["status_code"])
            self.assertTrue(response["status_code"] == 200)
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(
                        os.path.join(self.__unitTestFolder, "D_1000000001.tar.gz")
                    )
                ),
                "error - compressed file not found",
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCompressDirPath(self):
        try:
            mD = {"dirPath": os.path.join(self.__unitTestFolder, "D_1000000001")}
            response = self.__cU.compressDirPath(**mD)
            logger.info("file status %r", response["status_code"])
            self.assertTrue(response["status_code"] == 200)
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(
                        os.path.join(self.__unitTestFolder, "D_1000000001.tar.gz")
                    )
                ),
                "error - compressed file not found",
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testDecompressDir(self):
        try:
            mD = {"repositoryType": self.__repositoryType, "depId": "D_1000000001"}
            response = self.__cU.compressDir(**mD)
            logger.info("file status %r", response["status_code"])
            self.assertTrue(response["status_code"] == 200)
            response = self.__cU.decompressDir(**mD)
            logger.info("file status %r", response["status_code"])
            self.assertTrue(response["status_code"] == 200)
            self.assertTrue(
                os.path.exists(
                    os.path.abspath(os.path.join(self.__unitTestFolder, "D_1000000001"))
                ),
                "error - decompressed folder not found",
            )
            self.assertFalse(
                os.path.exists(
                    os.path.abspath(
                        os.path.join(self.__unitTestFolder, "D_1000000001.tar.gz")
                    )
                ),
                "error - compressed file not removed",
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testNextVersion(self):
        try:
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1000000001",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
            }
            response = self.__cU.nextVersion(**mD)
            logger.info("file status response %r", response["status_code"])
            self.assertTrue(response["status_code"] == 200, "error - status code %s" % response["status_code"])
            self.assertTrue(
                int(response["version"]) == 2,
                "error - returned wrong file version %s" % response["version"],
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testLatestVersion(self):
        try:
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1000000001",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
            }
            response = self.__cU.latestVersion(**mD)
            logger.info("file status response %r", response["status_code"])
            self.assertTrue(response["status_code"] == 200, "error - status code %s" % response["status_code"])
            self.assertTrue(
                int(response["version"]) == 1,
                "error - returned wrong file version %s" % response["version"],
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testFileExists(self):
        endPoint = "file-exists"
        try:
            # test response 200
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1000000001",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
                "version": 1,
            }
            response = self.__cU.fileExists(**mD)
            self.assertTrue(response["status_code"] == 200, "error - status code %d" % response["status_code"])
            logger.info("file status response %r", response["status_code"])
            # test response 404
            mD = {
                "repositoryType": self.__repositoryType,
                "depId": "D_1234567890",
                "contentType": "model",
                "milestone": None,
                "partNumber": 1,
                "contentFormat": "pdbx",
                "version": 1,
            }
            response = self.__cU.fileExists(**mD)
            self.assertTrue(response["status_code"] == 404, "error - status code %d" % response["status_code"])
            logger.info("file status response %r", response["status_code"])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testFileSize(self):
        logger.info("test file size")
        repoType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1
        parameters = {
            "repositoryType": repoType,
            "depId": depId,
            "contentType": contentType,
            "milestone": milestone,
            "partNumber": partNumber,
            "contentFormat": contentFormat,
            "version": version,
        }
        # test correct file size returned
        response = self.__cU.fileSize(**parameters)
        self.assertTrue(
            response["status_code"] == 200, f"error - 200 = {response['status_code']}"
        )
        fileSize = int(response["fileSize"])
        self.assertTrue(
            fileSize == self.__chunkSize, f"error - returned wrong file size {fileSize}"
        )


def client_tests():
    suite = unittest.TestSuite()
    suite.addTest(ClientTests("testSimpleUpload"))
    suite.addTest(ClientTests("testResumableUpload"))
    suite.addTest(ClientTests("testSimpleDownload"))
    suite.addTest(ClientTests("testChunkDownload"))
    suite.addTest(ClientTests("testFilePathLocal"))
    suite.addTest(ClientTests("testFilePathRemote"))
    suite.addTest(ClientTests("testListDir"))
    suite.addTest(ClientTests("testDirExists"))
    suite.addTest(ClientTests("testCopyFile"))
    suite.addTest(ClientTests("testCopyDir"))
    suite.addTest(ClientTests("testMoveFile"))
    suite.addTest(ClientTests("testCompressDir"))
    suite.addTest(ClientTests("testDecompressDir"))
    suite.addTest(ClientTests("testCompressDirPath"))
    suite.addTest(ClientTests("testLatestVersion"))
    suite.addTest(ClientTests("testNextVersion"))
    suite.addTest(ClientTests("testFileSize"))
    suite.addTest(ClientTests("testFileExists"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(client_tests())
