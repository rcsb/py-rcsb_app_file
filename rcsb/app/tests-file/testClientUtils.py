##
# File:    testClientUtils.py
# Author:  Dennis Piehl
# Date:    10-June-2022
# Version: 0.001
#
# Update:
#
#
##
"""
Tests for client utility wrapper for making API calls.

"""

__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest
import asyncio

from rcsb.app.file import __version__
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.app.file.ClientUtils import ClientUtils

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.propagate = False


class ClientUtilsTests(unittest.TestCase):
    def setUp(self):
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__testFilePath = os.path.join(self.__dataPath, "example-data.cif")
        # self.__testFilePath = os.path.join(self.__dataPath, "bigFile.txt.5gb")
        self.__testFileDownloadPath = os.path.join(HERE, "test-output", "example-data-download.cif")
        self.__cachePath = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
        self.__configFilePath = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))
        #
        fU = FileUtil()
        for fn in ["example-data.cif"]:  # Only needed for ConfigProvider init
            fU.put(self.__testFilePath, os.path.join(self.__cachePath, fn))
        #
        self.__cU = ClientUtils(cachePath=self.__cachePath, configFilePath=self.__configFilePath, hostAndPort="http://0.0.0.0:8000")
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

    async def gatherSingleFileUploadTasks(self, numTasks, sessionId, slice_size):
        # Schedule n single-file upload calls *concurrently*:
        tL = []
        for i in range(numTasks):
            tL.append(self.__cU.upload([{
                'filePath': self.__testFilePath,
                'idCode': "D_499900000"+str(i),
                'repositoryType': "onedep-archive",
                'contentType': "model",
                'contentFormat': "pdbx",
                'partNumber': 1,
                'version': "9",
                'copyMode': "native",
                'allowOverWrite': True,
                'sessionId': sessionId,
                'slice_size': slice_size
            }]))
        taskL = await asyncio.gather(*tL)
        logger.info("single-file upload taskL: %r", taskL)

    def testClientUtils(self):
        """Test - file upload, multipart upload, and file download"""
        try:
            # Test single-file single-part upload
            logger.info("Starting upload of file %s", self.__testFilePath)
            sessionId = asyncio.run(self.__cU.getSession())
            slice_size = 2500
            startTime = time.time()
            asyncio.run(
                self.__cU.upload([{
                    'filePath': self.__testFilePath,
                    'idCode': "D_4999000001",
                    'repositoryType': "onedep-archive",
                    'contentType': "model",
                    'contentFormat': "pdbx",
                    'partNumber': 1,
                    'version': 9,
                    'copyMode': "native",
                    'allowOverWrite': True,
                    'sessionId': sessionId,
                    'slice_size': slice_size
                }])
            )
            logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
            #
            # Test *concurrency* for multiple single-file single-part uploads
            logger.info("Starting concurrent single-file uploads")
            startTime = time.time()
            asyncio.run(self.gatherSingleFileUploadTasks(numTasks=8, sessionId=sessionId, slice_size=slice_size))
            logger.info("Completed concurrent upload (%.4f seconds)", time.time() - startTime)
            #
            # Test single-file multipart upload
            logger.info("Starting multipart-upload of file %s", self.__testFilePath)
            startTime = time.time()
            response = asyncio.run(
                self.__cU.upload([{
                    'filePath': self.__testFilePath,
                    'idCode': "D_8000210027",
                    'repositoryType': "onedep-archive",
                    'contentType': "model",
                    'contentFormat': "pdbx",
                    'partNumber': 1,
                    'version': "1",
                    'copyMode': "native",
                    'allowOverWrite': True,
                    'sessionId': sessionId,
                    'slice_size': slice_size
                }])
            )
            #
            logger.info("Completed multipart upload (%.4f seconds) %s", time.time() - startTime, str(response))
            #
            # Test multi-file multipart upload
            logger.info("Starting multi-file multipart-upload of file %s", self.__testFilePath)
            startTime = time.time()
            data = []
            for part in range(1, 4):
                data.append(
                    {
                        'filePath': self.__testFilePath,
                        'idCode': "D_8000210027",
                        'repositoryType': "onedep-archive",
                        'contentType': "model",
                        'contentFormat': "pdbx",
                        'partNumber': part,
                        'version': 1,
                        'copyMode': "native",
                        'allowOverWrite': True,
                        'sessionId': sessionId,
                        'slice_size': slice_size
                    }
                )
            results = asyncio.run(self.__cU.upload(data))
            for result in results:
                print(f'multi-file multipart result {result}')
            #
            logger.info("Completed multi-file multipart upload (%.4f seconds) %s", time.time() - startTime, str(result))
            #
            # Test file download
            logger.info("Starting download of last uploaded file to %s", self.__testFileDownloadPath)
            startTime = time.time()
            asyncio.run(
                self.__cU.download(
                    fileDownloadPath=self.__testFileDownloadPath,
                    idCode="D_4999000001",
                    repositoryType="onedep-archive",
                    contentType="model",
                    contentFormat="pdbx",
                    partNumber=1,
                    version="9",
                )
            )
            logger.info("Completed download (%.4f seconds)", time.time() - startTime)
            ok = asyncio.run(self.__cU.clearSession(sessionId))
            logger.info(f"Removed session {sessionId} with status %r", ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def clientUtilTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ClientUtilsTests("testClientUtils"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = clientUtilTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
