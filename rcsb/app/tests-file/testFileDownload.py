import re
import sys
import unittest
import os
import shutil
import logging
import time

from fastapi.testclient import TestClient
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider

logging.basicConfig(level=logging.INFO)

class DownloadTest(unittest.TestCase):
    def setUp(self):
        self.__cP = ConfigProvider()
        self.__baseUrl = self.__cP.get("SERVER_HOST_AND_PORT")
        self.__configFilePath = self.__cP.getConfigFilePath()
        subject = self.__cP.get("JWT_SUBJECT")
        self.__headerD = {
            "Authorization": "Bearer "
            + JWTAuthToken().createToken({}, subject)
        }
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__hashType = self.__cP.get("HASH_TYPE")
        self.__dataPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__repositoryType = "unit-test"
        self.__depId = "D_1000000001"
        self.__contentType = "model"
        self.__milestone = "upload"
        self.__convertedMilestone = ""
        if self.__milestone is not None and self.__milestone != "":
            self.__convertedMilestone = "-" + self.__milestone
        self.__partNumber = 1
        self.__contentFormat = "pdbx"
        self.__convertedContentFormat = None
        if self.__contentFormat == "pdbx":
            self.__convertedContentFormat = "cif"
        else:
            logging.error("error - please refer to upload functions for other examples of content format conversion")
            sys.exit()
        self.__version = "1"
        if not re.match(r"^\d+$", self.__version):
            logging.error("error - have not encoded logic for non-numeric version numbers")
            sys.exit()
        self.__repoFileName = f"{self.__depId}_{self.__contentType}{self.__convertedMilestone}_P{self.__partNumber}.{self.__convertedContentFormat}.V{self.__version}"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repositoryType)
        logging.info("self.__dataPath %s", self.__dataPath)

        self.__repositoryFile = os.path.join(self.__unitTestFolder, self.__depId, self.__repoFileName)
        if not os.path.exists(self.__repositoryFile):
            os.makedirs(os.path.dirname(self.__repositoryFile), mode=0o757, exist_ok=True)
            nB = self.__chunkSize * 2
            with open(self.__repositoryFile, "wb") as out:
                out.write(os.urandom(nB))
        if not os.path.exists(self.__repositoryFile):
            logging.error("error - could not make repo file")
            sys.exit()
        self.__downloadFile = os.path.join(self.__dataPath, "downloadFile.dat")
        if os.path.exists(self.__downloadFile):
            os.unlink(self.__downloadFile)

        self.__startTime = time.time()
        logging.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if os.path.exists(self.__repositoryFile):
            os.unlink(self.__repositoryFile)
        if os.path.exists(self.__downloadFile):
            os.unlink(self.__downloadFile)
        # warning - do not delete the data/repository folder for production, just the unit-test folder within that folder
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)
        endTime = time.time()
        logging.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testSimpleDownload(self):
        downloadUrlPrefix = os.path.join(self.__baseUrl, "file-v1", "download")
        downloadUrl = (
            f"{downloadUrlPrefix}?repositoryType={self.__repositoryType}&depId={self.__depId}&contentType={self.__contentType}&milestone={self.__milestone}"
            f"&partNumber={self.__partNumber}&contentFormat={self.__contentFormat}&version={self.__version}&hashType={self.__hashType}"
        )
        resp = None
        # test download file, return http response
        with TestClient(app) as client:
            response = client.get(
                downloadUrl, headers=self.__headerD, timeout=None
            )
            if response and response.status_code == 200:
                with open(self.__downloadFile, "wb") as ofh:
                    ofh.write(response.content)
                rspHashType = response.headers["rcsb_hash_type"]
                rspHashDigest = response.headers["rcsb_hexdigest"]
                thD = CryptUtils().getFileHash(
                    self.__downloadFile, hashType=rspHashType
                )
                self.assertTrue(thD["hashDigest"] == rspHashDigest, f"error - hash comparison failed")
                if not thD["hashDigest"] == rspHashDigest:
                    logging.error("Hash comparison failed")
                    return None
                self.assertTrue(response.status_code == 200, f"error - status code {response.status_code}")
                resp = response.status_code

        return resp

def download_tests():
    suite = unittest.TestSuite()
    suite.addTest(DownloadTest("testSimpleDownload"))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(download_tests())
