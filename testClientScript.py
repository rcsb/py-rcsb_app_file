import asyncio
import uuid
import requests
import os
import random
import time

from rcsb.app.file.IoUtils import IoUtils

os.environ["CACHE_PATH"] = os.path.join("app", "CACHE")

from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from fastapi.testclient import TestClient
from rcsb.app.file.main import app

cachePath = os.environ.get("CACHE_PATH")
cP = ConfigProvider(cachePath)
cD = {
    "JWT_SUBJECT": "aTestSubject",
    "JWT_ALGORITHM": "HS256",
    "JWT_SECRET": "aTestSecret",
    "SESSION_DIR_PATH": os.path.join(cachePath, "sessions"),
    "REPOSITORY_DIR_PATH": os.path.join(cachePath, "repository"),
    "SHARED_LOCK_PATH": os.path.join(cachePath, "shared-locks"),
    }
cP.setConfig(configData=cD)

repositoryPath = cP.get("REPOSITORY_DIR_PATH")
sessionPath = cP.get("SESSION_DIR_PATH")
subject = cP.get("JWT_SUBJECT")

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

filePath = "./rcsb/app/tests-file/test-data/testFile.dat"
cachePath = "app/CACHE/"

#create file for download
#select size of file here (in bytes)
nB = 100
with open(filePath, "wb") as ofh:
    ofh.write(os.urandom(nB))

fU = FileUtil()
fU.remove(repositoryPath)
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
testHash = hD["hashDigest"]
headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath).createToken({}, subject)}


partNumber = 1
copyMode = "native"
allowOverWrite = True

#uploads the file x times
for version in range(1, 10):

    mD = {
    "idCode": "D_00000000",
    "repositoryType": "onedep-archive",
    "contentType": "model",
    "contentFormat": "cif",
    "partNumber": partNumber,
    "version": str(version),
    "copyMode": copyMode,
    "allowOverWrite": allowOverWrite,
    "hashType": hashType,
    "hashDigest": testHash
    }

    url = "http://0.0.0.0:80/file-v1/upload"

    #upload with requests library
    with open(filePath, "rb") as ifh:
        files = {"uploadFile": ifh}
        response = requests.post(url, files = files, data = mD, headers = headerD)
    print(response.text)

for version in range(1, 10):
    
    downloadDict = {
    "idCode": "D_00000000",
    "repositoryType": "onedep-archive",
    "contentType": "model",
    "contentFormat": "cif",
    "partNumber": partNumber,
    "version": str(version),
    "hashType": hashType,
    }
    #set file download path
    downloadFilePath = "./test-output/" + downloadDict["idCode"] + "/" + downloadDict["idCode"] + "_" + downloadDict["version"] + ".dat"
    downloadDirPath = "./test-output/" + downloadDict["idCode"] + "/"
    downloadName = downloadDict["idCode"] + "_" + "v" + downloadDict["version"]
    FileUtil().mkdir(downloadDirPath)

    url = "http://0.0.0.0:80/file-v1/download/onedep-archive"

    #upload with requests library
    response = requests.get(url, params = downloadDict, headers = headerD)

    with open(downloadFilePath, "wb") as ofh:
        ofh.write(response.content)

#sliced upload
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType = hashType)
fullTestHash = hD["hashDigest"]

url = "http://0.0.0.0:80/file-v1/upload-slice"

cP = ConfigProvider(cachePath)
ioU = IoUtils(cP)
sessionId = uuid.uuid4().hex

sliceTotal = 4
loop = asyncio.get_event_loop()
task = ioU.splitFile(filePath, sliceTotal, "staging" + sessionId)
sP = loop.run_until_complete(task)

sliceIndex = 0
manifestPath = os.path.join(sP, "MANIFEST")
with open(manifestPath, "r", encoding = "utf-8") as ifh:
    for line in ifh:
        testFile = line[:-1]
        testFilePath = os.path.join(sP, testFile)
        sliceIndex += 1
        
        mD = {
            "sliceIndex": sliceIndex,
            "sliceTotal": sliceTotal,
            "sessionId": sessionId,
            "copyMode": "native",
            "allowOverWrite": True,
            "hashType": None,
            "hashDigest": None,
        }

        with open(testFilePath, "rb") as ifh:
            files = {"uploadFile": ifh}
            response = requests.post(url, files = files, data = mD, headers = headerD)

partNumber = 1
allowOverWrite = True
version = 1

mD = {
    "sessionId": sessionId,
    "sliceTotal": sliceTotal,
    "idCode": "D_00000000",
    "repositoryType": "onedep-archive",
    "contentType": "model",
    "contentFormat": "cif",
    "partNumber": partNumber,
    "version": str(version),
    "copyMode": "native",
    "allowOverWrite": allowOverWrite,
    "hashType": hashType,
    "hashDigest": fullTestHash,
}

url = "http://0.0.0.0:80/file-v1/join-slice"

with open(testFilePath, "rb") as ifh:
    response = requests.post(url, data = mD, headers = headerD)
