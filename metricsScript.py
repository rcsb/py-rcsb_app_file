#!/usr/bin/python

import asyncio
from doctest import testfile
import uuid
import requests
import os
import random
import string
import logging
import time

from rcsb.app.file.IoUtils import IoUtils

os.environ["CACHE_PATH"] = "app/CACHE/"

from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from fastapi.testclient import TestClient
from rcsb.app.file.main import app

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

logger = logging.getLogger()

filePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/testFile.dat"
cachePath = "app/CACHE/"

#create file for download
#select size of file here (in bytes)
nB = 25000000
with open(filePath, "wb") as ofh:
    ofh.write(os.urandom(nB))


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
fU = FileUtil()
fU.remove(repositoryPath)
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
testHash = hD["hashDigest"]

partNumber = 1
copyMode = "native"
allowOverWrite = True

uploadTimeList = []

#uploads the file 9 times
for version in range(1, 10):
    startTime = time.time()
    
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

    headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath).createToken({}, subject)}
    url = "http://0.0.0.0:80/file-v1/upload"
    awsurl = "http://3.94.78.165:80/file-v1/upload" #change this to match aws public ipv4

    #upload with requests library
    with open(filePath, "rb") as ifh:
        files = {"uploadFile": ifh}
        #comment out this line to use aws server
        response = requests.post(url, files = files, data = mD, headers = headerD)
        #comment out this line to use local server
        #response = requests.post(awsurl, files = files, data = mD, headers = headerD)
    print(response.text)

    timeSpent = time.time() - startTime
    logger.info("Completed %s (%.4f seconds)", mD["idCode"], time.time() - startTime)
    uploadTimeList.append(timeSpent)

#get average upload time
uploadSum = sum(uploadTimeList)
uploadAvg = uploadSum / len(uploadTimeList)
print("\n" "Average upload time", uploadAvg, "seconds", "\n"
"Size:", os.path.getsize(filePath), "bytes" "\n")


downloadTimeList = []

for version in range(1, 10):
    startTime = time.time()
    
    downloadDict = {
    "idCode": "D_00000000",
    "repositoryType": "onedep-archive",
    "contentType": "model",
    "contentFormat": "cif",
    "partNumber": partNumber,
    "version": str(version),
    "hashType": hashType,
    }

    downloadFilePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/" + downloadDict["idCode"] + "/" + downloadDict["idCode"] + "_" + downloadDict["version"] + ".dat"
    downloadDirPath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/" + downloadDict["idCode"] + "/"
    downloadName = downloadDict["idCode"] + "_" + "v" + downloadDict["version"]
    FileUtil().mkdir(downloadDirPath)


    headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath).createToken({}, subject)}
    url = "http://0.0.0.0:80/file-v1/download/onedep-archive"
    awsurl = "http://3.94.78.165:80/file-v1/download/onedep-archive" #change this to match aws public ipv4

    #upload with requests library
    #comment out this line to use aws server
    response = requests.get(url, params = downloadDict, headers = headerD)
    #comment out this line to use local server
    #response = requests.get(awsurl, params = downloadDict, headers = headerD)

    with open(downloadFilePath, "wb") as ofh:
        ofh.write(response.content)

    timeSpent = time.time() - startTime
    logger.info("Completed %s (%.4f seconds)", downloadName, time.time() - startTime)
    downloadTimeList.append(timeSpent)

#get average download time
downloadSum = sum(downloadTimeList)
downloadAvg = downloadSum / len(downloadTimeList)
print("\n" "Average download time", downloadAvg, "seconds", "\n"
"Size:", os.path.getsize(filePath), "bytes" "\n")


#sliced upload
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType = hashType)
fullTestHash = hD["hashDigest"]

url = "http://0.0.0.0:80/file-v1/upload-slice"
awsurl = "http://3.94.78.165:80/file-v1/upload-slice" #change this to match aws public ipv4

cP = ConfigProvider(cachePath)
ioU = IoUtils(cP)
sessionId = uuid.uuid4().hex

sliceTotal = 4
loop = asyncio.get_event_loop()
task = ioU.splitFile(filePath, sliceTotal, "staging" + sessionId, hashType = "MD5")
sP = loop.run_until_complete(task)

sliceTimeList = []
sliceIndex = 0
manifestPath = os.path.join(sP, "MANIFEST")
with open(manifestPath, "r", encoding = "utf-8") as ifh:
    for line in ifh:
        testFile = line[:-1]
        testFilePath = os.path.join(sP, testFile)
        sliceIndex += 1
        startTime = time.time()
        
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
            #response = requests.post(awsurl, files = files, data = mD, headers = headerD)

        print("Slice", sliceIndex, "in:", str(time.time() - startTime), "seconds")
        sliceTimeList.append(time.time() - startTime)

sliceSum = sum(sliceTimeList)
sliceAvg = sliceSum / len(sliceTimeList)
print("\n" "Average slice upload time", sliceAvg, "seconds", "\n"
"Size:", os.path.getsize(filePath), "bytes" "\n")


startTime = time.time()
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
awsurl = "http://3.94.78.165:80/file-v1/join-slice" #change this to match aws public ipv4

with open(testFilePath, "rb") as ifh:
    response = requests.post(url, data = mD, headers = headerD)
    #response = requests.post(awsurl, data = mD, header = headerD)
print("Slices joined in:", str(time.time() - startTime), "seconds\n")