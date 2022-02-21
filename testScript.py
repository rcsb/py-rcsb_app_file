#!/usr/bin/python

import requests
import os
import random
import string
import logging

os.environ["CACHE_PATH"] = "app/CACHE/"

from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from fastapi.testclient import TestClient
from rcsb.app.file.main import app

filePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-output/CACHE/sessions/testFile.dat"
#cachePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/CACHE"

cachePath = "app/CACHE/"

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

subject = cP.get("JWT_SUBJECT")
repositoryPath = cP.get("REPOSITORY_DIR_PATH")
sessionPath = cP.get("SESSION_DIR_PATH")
fU = FileUtil()
fU.remove(repositoryPath)
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
testHash = hD["hashDigest"]

partNumber = 1
version = 1
copyMode = "native"
allowOverWrite = True

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
awsurl = "http://3.82.136.82:80/file-v1/upload" #change this to match aws public ipv4

#upload with requests library
with open(filePath, "rb") as ifh:
    files = {"uploadFile": ifh}
    #comment out this line to use aws server
    req = requests.post(url, files = files, data = mD, headers = headerD)
    #comment out this line to use local server
    #req = requests.post(awsurl, files = files, data = mD, headers = headerD)
print(req.text)


# test upload using FastAPI TestClient
# with TestClient(app) as client:
#     with open(filePath, "rb") as ifh:
#         files = {"uploadFile": ifh}
#         response = client.post("/file-v1/upload", files = files, data = mD, headers = headerD)

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

fU = FileUtil()
fU.mkdir(sessionPath)

sessionPath = os.path.join(cachePath, "sessions")
repoPath = os.path.join(cachePath, "repository", "archive")
testFilePath = os.path.join(sessionPath, "testFile.dat")

#creates testFile.dat, adds random data
nB = 2500000
with open(testFilePath, "w", encoding="utf-8") as ofh:
    ofh.write("".join(random.choices(string.ascii_uppercase + string.digits, k=nB)))

#creates directory for "D_1000000001", and "D_1000000002", creates files using data from testFile.dat
for idCode in ["D_1000000001", "D_1000000002"]:
    dirPath = os.path.join(repoPath, idCode)
    FileUtil().mkdir(dirPath)
    for pNo in ["P1", "P2"]:
        for contentType, fmt in ctFmtTupL[:6]:
            for vS in ["V1", "V2"]:
                fn = idCode + "_" + contentType + "_" + pNo + "." + fmt + "." + vS
                pth = os.path.join(dirPath, fn)
                FileUtil().put(testFilePath, pth)  

refHashType = refHashDigest = "MD5"

#use this download dict to test downloading file that was uploaded previously in this script(using requests library)
downloadDict = {
    "idCode": "D_00000000",
                    "contentType": "model",
                    "contentFormat": "cif",
                    "partNumber": 1,
                    "version": 1,
                    "hashType": refHashType,
}
#use this downloadDict for local download test
# downloadDict = {
#     "idCode": "D_1000000001",
#                     "contentType": "model",
#                     "contentFormat": "cif",
#                     "partNumber": 1,
#                     "version": 1,
#                     "hashType": refHashType,
#                     "fileName": "testFile"
# }

#path to download file
downloadFilePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/" + downloadDict["idCode"] + ".dat"
print(downloadFilePath)
useHash = True
if useHash:
    refHashType = "MD5"
    hD = CryptUtils().getFileHash(testFilePath, hashType = refHashType)
    refHashDigest = hD["hashDigest"]

url = "http://0.0.0.0:80/file-v1/download/onedep-archive"
#download with requests library
response = requests.get(url, params=downloadDict, headers=headerD)
rspHashType = response.headers["rcsb_hash_type"]
rspHashDigest = response.headers["rcsb_hexdigest"]
print(rspHashDigest, refHashDigest)
with open(downloadFilePath, "wb") as ofh:
    ofh.write(response.content)
thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
if thD["hashDigest"] == rspHashDigest:
    print("rspHashDigest")
if thD["hashDigest"] == refHashDigest:
    print("refHashDigest")


#download with FastAPI TestClient
# with TestClient(app) as client:
#     response = client.get("/file-v1/download/onedep-archive", params=downloadDict, headers=headerD)
#     rspHashType = response.headers["rcsb_hash_type"]
#     rspHashDigest = response.headers["rcsb_hexdigest"]
#     print(rspHashDigest, refHashDigest)
#     with open(downloadFilePath, "wb") as ofh:
#         ofh.write(response.content)
#     thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
#     if thD["hashDigest"] == rspHashDigest:
#         print("rspHashDigest")
#     if thD["hashDigest"] == refHashDigest:
#         print("refHashDigest")