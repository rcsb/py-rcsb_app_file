#!/usr/bin/python

import requests
import os

HERE = os.path.abspath(os.path.dirname(__file__))
print(HERE)
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
print(TOPDIR)
os.environ["CACHE_PATH"] = os.path.join(HERE, "test-output", "CACHE")
cachePath = os.environ.get("CACHE_PATH")
print(os.environ["CACHE_PATH"])
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from fastapi.testclient import TestClient
from rcsb.app.file.main import app


filePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/CACHE/sessions/testFile.dat"
#cachePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/CACHE"

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
    "allowOverwrite": allowOverWrite,
    "hashType": hashType,
    "hashDigest": testHash
    }

headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath).createToken({}, subject)}
url = "http://0.0.0.0:80/file-v1/upload"


with open(filePath, "rb") as ifh:
    files = {"uploadFile": ifh}
    req = requests.post(url, files = files, data = mD, headers = headerD)
print(req.text)

# with TestClient(app) as client:
#     with open(filePath, "rb") as ifh:
#         files = {"uploadFile": ifh}
#         response = client.post("/file-v1/upload", files = files, data = mD, headers = headerD)
