#!/usr/bin/python

import requests
import os
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider


filePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/CACHE/sessions/testFile.dat"
cachePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/CACHE"

cP = ConfigProvider("/Users/cparker/RCSBWork/py-rcsb_app_file/test-output/CACHE")
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
