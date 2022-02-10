#File for executing commands on docker container using python
#Connor Parker
#February 4, 2022
#connor.parker@rcsb.org


import asyncio
import gzip
import logging
import os
import platform
import resource
import time
import unittest
import uuid

from numpy import true_divide

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))

from fastapi.testclient import TestClient
from rcsb.app.file import __version__
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.main import app
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.LogUtil import StructFormatter

class DockerFileUpload():
    def setUp(self):
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__cachePath = os.environ.get("CACHE_PATH", os.path.join(HERE, "test-output", "CACHE"))
        self.__sessionPath = os.path.join(self.__cachePath, "sessions")
        self.__fU = FileUtil()
        self.__fU.remove(self.__sessionPath)
        self.__fU.mkdir(self.__sessionPath)

        nB = 25000000
        self.__testFilePath = os.path.join(self.__sessionPath, "testFile.dat")
        with open(self.__testFilePath, "wb") as ofh:
            ofh.write(os.urandom(nB))  # generate random content file

        self.__testFileGzipPath = os.path.join(self.__sessionPath, "testFile.dat.gz")
        with gzip.open(self.__testFileGzipPath, "wb") as ofh:
            with open(self.__testFilePath, "rb") as ifh:
                ofh.write(ifh.read())

        cP = ConfigProvider(self.__cachePath)
        self.__cD = {
            "JWT_SUBJECT": "aTestSubject",
            "JWT_ALGORITHM": "HS256",
            "JWT_SECRET": "aTestSecret",
            "SESSION_DIR_PATH": os.path.join(self.__cachePath, "sessions"),
            "REPOSITORY_DIR_PATH": os.path.join(self.__cachePath, "repository"),
            "SHARED_LOCK_PATH": os.path.join(self.__cachePath, "shared-locks"),
        }
        cP.setConfig(configData=self.__cD)
        
        
        subject = cP.get("JWT_SUBJECT")
        self.__headerD = {"Authorization": "Bearer " + JWTAuthToken(self.__cachePath).createToken({}, subject)}
        # clear any previous data
        self.__repositoryPath = cP.get("REPOSITORY_DIR_PATH")
        self.__fU.remove(self.__repositoryPath)
        #
        self.__startTime = time.time()
        #

    def uploadScript(self):
        hashType = testHash = None
        endPoint = "upload"
        hashType = "MD5"
        version = 1
        testFilePath = self.__testFilePath

        hD = CryptUtils().getFileHash(self.__testFilePath, hashType=hashType)
        testHash = hD["hashDigest"]

        copyMode = "native"
        partNumber = 1
        allowOverWrite = False

        mD = {
            "idCode": "D_00000000",
                        "repositoryType": "onedep-archive",
                        "contentType": "model",
                        "contentFormat": "cif",
                        "partNumber": partNumber,
                        "version": str(version),
                        "copyMode": copyMode,
                        "allowOverwrite": allowOverWrite,
                        "hashType": hashType,
                        "hashDigest": testHash,
                    }

        responseCode = 200

        with TestClient(app) as client:
                        with open(testFilePath, "rb") as ifh:
                            files = {"uploadFile": ifh}
                            response = client.post("/file-v1/%s" % endPoint, files=files, data=mD, headers=self.__headerD)
                        if response.status_code != responseCode:
                            print()
                        rD = response.json()
                        if responseCode == 200:
                            print()


        print("Upload Script Complete")



Test = DockerFileUpload()
Test.setUp()
Test.uploadScript()
