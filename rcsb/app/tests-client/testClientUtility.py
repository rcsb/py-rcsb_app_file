##
# File - testClientUtility.py
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
import math
import resource
import unittest
import shutil
import filecmp
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter
from rcsb.app.client.ClientUtility import ClientUtility
from rcsb.app.file.IoUtility import IoUtility
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

        self.__cU = ClientUtility()
        self.__cP = ConfigProvider()
        self.__fU = FileUtil()

        self.__configFilePath = self.__cP.getConfigFilePath()
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__fileSize = self.__chunkSize * 2
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
        if not os.path.exists(self.__unitTestFolder2):
            os.makedirs(self.__unitTestFolder2)
        if not os.path.exists(self.__repositoryFile1):
            os.makedirs(
                os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True
            )
            nB = self.__fileSize
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
        if not os.path.exists(self.__repositoryFile2):
            os.makedirs(
                os.path.dirname(self.__repositoryFile2), mode=0o757, exist_ok=True
            )
            nB = self.__fileSize
            with open(self.__repositoryFile2, "wb") as out:
                out.write(os.urandom(nB))
        if not os.path.exists(self.__repositoryFile3):
            os.makedirs(
                os.path.dirname(self.__repositoryFile3), mode=0o757, exist_ok=True
            )
            nB = self.__fileSize
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
            nB = self.__fileSize
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

        try:
            # return 200
            partNumber = 1
            decompress = False
            allowOverwrite = True
            fileExtension = os.path.splitext(self.__testFileDatPath)[-1]
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
                resumable,
            )
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}"
            )
            self.assertTrue(
                response["status_code"] == 200,
                "error - status code %s" % response["status_code"],
            )

            # return 200
            partNumber = 2
            fileExtension = os.path.splitext(self.__testFileDatPath)[-1]
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
                resumable,
            )
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}"
            )
            self.assertTrue(response["status_code"] == 200)

            # return 403 (file already exists)
            partNumber = 1
            allowOverwrite = False
            fileExtension = os.path.splitext(self.__testFileDatPath)[-1]
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
                resumable,
            )
            logger.info(
                f"{PathProvider().getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)} decompress {decompress} overwrite {allowOverwrite}"
            )
            self.assertTrue(response["status_code"] == 403)

            # return 200 (decompress gzip file)
            partNumber = 3
            decompress = True
            allowOverwrite = True
            fileExtension = os.path.splitext(self.__testFileGzipPath)[-1]
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
                resumable,
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
        self.assertTrue(os.path.exists(self.__testFileDatPath))

        sourceFilePath = self.__testFileDatPath
        repositoryType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = ""
        partNumber = 1
        contentFormat = "pdbx"
        version = "next"
        allowOverwrite = False
        resumable = True
        decompress = False

        # get upload parameters
        response = self.__cU.getUploadParameters(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            allowOverwrite,
            resumable,
        )
        self.assertTrue(
            response and response["status_code"] == 200,
            "error in get upload parameters %r" % response,
        )
        saveFilePath = response["filePath"]
        chunkIndex = response["chunkIndex"]
        uploadId = response["uploadId"]
        self.assertTrue(chunkIndex == 0, "error - chunk index %s" % chunkIndex)
        # compress (externally), then hash, then upload
        # hash
        hashType = self.__hashType
        fullTestHash = IoUtility().getHashDigest(sourceFilePath, hashType=hashType)
        # compute expected chunks
        fileSize = os.path.getsize(sourceFilePath)
        chunkSize = int(self.__chunkSize)
        expectedChunks = 1
        if chunkSize < fileSize:
            expectedChunks = math.ceil(fileSize / chunkSize)
        fileExtension = os.path.splitext(self.__testFileDatPath)[-1]

        # upload chunks sequentially
        mD = {
            # chunk parameters
            "chunkSize": chunkSize,
            "chunkIndex": chunkIndex,
            "expectedChunks": expectedChunks,
            # upload file parameters
            "uploadId": uploadId,
            "hashType": hashType,
            "hashDigest": fullTestHash,
            # save file parameters
            "saveFilePath": saveFilePath,
            "decompress": decompress,
            "fileExtension": fileExtension,
            "allowOverwrite": allowOverwrite,
            "resumable": resumable,
        }
        status = None
        # upload one chunk
        for index in range(chunkIndex, chunkIndex + 1):
            mD["chunkIndex"] = index
            status = self.__cU.uploadChunk(sourceFilePath, fileSize, **mD)
            self.assertTrue(status == 200, "error in upload %r" % response)
            logger.info("uploaded chunk %d", index)

        # get resumed upload
        response = self.__cU.getUploadParameters(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
            allowOverwrite,
            resumable,
        )
        self.assertTrue(
            response and response["status_code"] == 200,
            "error in get upload parameters %r" % response,
        )
        saveFilePath = response["filePath"]
        chunkIndex = response["chunkIndex"]
        uploadId = response["uploadId"]
        self.assertTrue(chunkIndex > 0, "error - chunk index %s" % chunkIndex)
        logger.info("resumed upload on chunk %d", chunkIndex)

        # upload remaining chunks
        for index in range(chunkIndex, expectedChunks):
            mD["chunkIndex"] = index
            status = self.__cU.uploadChunk(sourceFilePath, fileSize, **mD)
            self.assertTrue(status == 200, "error in upload %r" % response)
            logger.info("uploaded remaining chunk %d", index)

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
            self.assertTrue(
                response["status_code"] == 404,
                "error - status code %d" % response["status_code"],
            )
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

        chunkSize = self.__chunkSize
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
            chunkIndex,
        )
        logger.info(
            f"{PathProvider().getFileName(depId,contentType,milestone,partNumber,contentFormat,version)} 200 = {response['status_code']}"
        )
        self.assertTrue(
            response["status_code"] == 200,
            "error - status code %d" % response["status_code"],
        )
        fileSize = os.path.getsize(self.__downloadFile)
        self.assertTrue(
            fileSize == chunkSize,
            "error - file size %d chunk size %d" % (fileSize, chunkSize),
        )

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
            self.assertTrue(
                response["status_code"] == 200,
                "error - status code %s" % response["status_code"],
            )
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
            self.assertTrue(
                response["status_code"] == 200,
                "error - status code %s" % response["status_code"],
            )
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
            self.assertTrue(
                response["status_code"] == 200,
                "error - status code %d" % response["status_code"],
            )
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
            self.assertTrue(
                response["status_code"] == 404,
                "error - status code %d" % response["status_code"],
            )
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
            fileSize == self.__fileSize, f"error - returned wrong file size {fileSize}"
        )

    def testFileObject(self):
        repoType = self.__repositoryType
        depId = "D_1000000001"
        contentType = "model"
        milestone = None
        partNumber = 1
        contentFormat = "pdbx"
        version = 1
        sessionDir = self.__unitTestFolder2
        # path of file after download
        download_file_path = os.path.join(
            sessionDir,
            PathProvider().getFileName(
                depId, contentType, milestone, partNumber, contentFormat, version
            ),
        )
        logger.info("downloaded file path %s", download_file_path)

        def reset():
            logger.info("test file object")
            # reset self.__repositoryFile1 to original size
            if not os.path.exists(os.path.dirname(self.__repositoryFile1)):
                os.makedirs(
                    os.path.dirname(self.__repositoryFile1), mode=0o757, exist_ok=True
                )
            if os.path.exists(self.__repositoryFile1):
                os.unlink(self.__repositoryFile1)
            nB = self.__fileSize
            with open(self.__repositoryFile1, "wb") as out:
                out.write(os.urandom(nB))
            self.assertTrue(
                os.path.exists(sessionDir),
                "error - session dir not created %s" % sessionDir,
            )
            self.assertTrue(
                os.path.getsize(self.__repositoryFile1) == self.__fileSize,
                f"error - file size {os.path.getsize(self.__repositoryFile1)}",
            )

        def testFileObject1():
            # test response success
            with self.__cU.getFileObject(
                repoType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                sessionDir,
            ) as file:
                self.assertTrue(file is not None, f"error - null response")
                self.assertTrue(
                    os.path.exists(download_file_path),
                    f"error - file path does not exist {download_file_path}",
                )
                self.assertTrue(
                    os.path.getsize(download_file_path)
                    == os.path.getsize(self.__repositoryFile1),
                    f"error - wrong file size {os.path.getsize(download_file_path)} != {self.__fileSize}",
                )
                self.assertTrue(
                    os.path.getsize(download_file_path) == self.__fileSize,
                    f"error - wrong file size {os.path.getsize(download_file_path)} != {self.__fileSize}",
                )
                self.assertTrue(
                    filecmp.cmp(download_file_path, self.__repositoryFile1),
                    f"error - files differ %s %s"
                    % (download_file_path, self.__repositoryFile1),
                )
            self.assertFalse(
                os.path.exists(download_file_path),
                f"error - file path still exists {download_file_path}",
            )

        def testFileObject2():
            # test read file
            backup_file = os.path.join(sessionDir, "backup")
            with self.__cU.getFileObject(
                repoType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                sessionDir,
            ) as file:
                self.assertTrue(file is not None, f"error - null response")
                self.assertTrue(
                    os.path.exists(download_file_path),
                    f"error - file path does not exist {download_file_path}",
                )
                self.assertTrue(
                    os.path.getsize(download_file_path) == self.__fileSize,
                    f"error - wrong file size {os.path.getsize(download_file_path)}",
                )
                self.assertTrue(
                    filecmp.cmp(download_file_path, self.__repositoryFile1),
                    f"error - files differ %s %s"
                    % (download_file_path, self.__repositoryFile1),
                )
                file.seek(0)
                with open(backup_file, "wb") as w:
                    w.write(file.read())
                self.assertTrue(
                    filecmp.cmp(backup_file, download_file_path),
                    f"error - files differ {backup_file} {download_file_path}",
                )
                self.assertTrue(
                    filecmp.cmp(backup_file, self.__repositoryFile1),
                    f"error - files differ {backup_file} {download_file_path}",
                )
                self.assertFalse(
                    filecmp.cmp(backup_file, self.__repositoryFile2),
                    f"error - filecmp not working",
                )
            self.assertFalse(
                os.path.exists(download_file_path),
                f"error - file path still exists {download_file_path}",
            )

        def testFileObject3():
            # test write to file
            # create test file with size 1.5x original file
            test_file = os.path.join(self.__unitTestFolder2, "test_file")
            with open(self.__repositoryFile1, "rb") as r:
                with open(test_file, "wb") as w:
                    w.write(r.read())
            test_chunk = os.path.join(self.__unitTestFolder2, "test_chunk")
            with open(test_chunk, "wb") as w:
                w.write(os.urandom(self.__chunkSize))
            with open(test_chunk, "rb") as r:
                with open(test_file, "ab") as w:
                    w.write(r.read())
            with self.__cU.getFileObject(
                repoType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                sessionDir,
            ) as file:
                self.assertTrue(file is not None, f"error - null response")
                self.assertTrue(
                    os.path.exists(download_file_path),
                    f"error - file path does not exist {download_file_path}",
                )
                self.assertTrue(
                    os.path.getsize(download_file_path) == self.__fileSize,
                    f"error - wrong file size {os.path.getsize(download_file_path)}",
                )
                self.assertTrue(
                    filecmp.cmp(download_file_path, self.__repositoryFile1),
                    f"error - files differ %s %s"
                    % (download_file_path, self.__repositoryFile1),
                )
                # seek to end of file prior to writing
                file.seek(os.path.getsize(download_file_path))
                # append chunk to end of file
                with open(test_chunk, "rb") as r:
                    file.write(r.read())
                self.assertTrue(
                    os.path.getsize(download_file_path)
                    == self.__fileSize + self.__chunkSize,
                    f"error - wrong file size {os.path.getsize(download_file_path)}",
                )
                # verify that chunk was written to end of file rather than start of file
                self.assertTrue(
                    filecmp.cmp(test_file, download_file_path),
                    f"error - files differ {test_file} {download_file_path}",
                )
            self.assertFalse(
                os.path.exists(download_file_path),
                f"error - file path still exists {download_file_path}",
            )
            # assert remote file overwritten
            self.assertTrue(
                os.path.getsize(self.__repositoryFile1)
                == self.__fileSize + self.__chunkSize,
                f"error - wrong file size {os.path.getsize(self.__repositoryFile1)}",
            )

        def testFileObject4():
            # test what happens when download file already exists (should overwrite, not append, then return handle to file with expected size)
            file_to_overwrite = os.path.join(self.__unitTestFolder2, "overwritable")
            with open(self.__repositoryFile1, "rb") as r:
                with open(file_to_overwrite, "wb") as w:
                    w.write(r.read())
            os.replace(file_to_overwrite, download_file_path)
            self.assertFalse(
                os.path.exists(file_to_overwrite),
                f"error - file path still exists {file_to_overwrite}",
            )
            self.assertTrue(
                os.path.exists(download_file_path),
                f"error - file path does not exist {download_file_path}",
            )
            self.assertTrue(
                os.path.getsize(download_file_path) == self.__fileSize,
                f"error - wrong file size {os.path.getsize(download_file_path)}",
            )
            with self.__cU.getFileObject(
                repoType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                sessionDir,
            ) as file:
                self.assertTrue(file is not None, f"error - null response")
                self.assertTrue(
                    os.path.exists(download_file_path),
                    f"error - file path does not exist {download_file_path}",
                )
                self.assertTrue(
                    os.path.getsize(download_file_path) == self.__fileSize,
                    f"error - wrong file size {os.path.getsize(download_file_path)}",
                )
                self.assertTrue(
                    filecmp.cmp(download_file_path, self.__repositoryFile1),
                    f"error - files differ %s %s"
                    % (download_file_path, self.__repositoryFile1),
                )
            self.assertFalse(
                os.path.exists(download_file_path),
                f"error - file path still exists {download_file_path}",
            )

        def testFileObject5():
            # test response failure (file not found)
            version = 4
            with self.__cU.getFileObject(
                repoType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                sessionDir,
            ) as file:
                self.assertTrue(file is None, f"error - expected null response")
                self.assertFalse(
                    os.path.exists(download_file_path),
                    f"error - file path exists {download_file_path}",
                )

        reset()
        testFileObject1()
        reset()
        testFileObject2()
        reset()
        testFileObject3()
        reset()
        testFileObject4()
        reset()
        testFileObject5()


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
    suite.addTest(ClientTests("testFileObject"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(client_tests())
