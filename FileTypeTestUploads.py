#!/usr/bin/python

import requests
import os
import logging

os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("rcsb", "app", "data"))
# os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("app", "CACHE"))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join("rcsb", "app", "config", "config.yml"))

from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider

cachePath = os.environ.get("CACHE_PATH")
configFilePath = os.environ.get("CONFIG_FILE")

logger = logging.getLogger()

cifFilePath = "./rcsb/app/tests-file/test-data/example-data.cif"
valReportFilePath = "./rcsb/app/tests-file/test-data/1cbs_validation.pdf"
EMMapFilePath = "./rcsb/app/tests-file/test-data/emd_32684.map"

cP = ConfigProvider(cachePath)
headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath, configFilePath).createToken({}, "aTestSubject")}

mD = {
    "idCode": "1cbs",
    "repositoryType": "onedep-archive",
    "contentType": "validation-report",
    "contentFormat": "pdf",
    "partNumber": 1,
    "version": 1,
    "copyMode": "native",
    "allowOverWrite": True,
}

url = "http://0.0.0.0:80/file-v1/upload"
awsurl = "http://34.239.121.66:80/file-v1/upload"  # change this to match aws public ipv4

# upload with requests library
with open(cifFilePath, "rb") as ifh:
    files = {"uploadFile": ifh}
    response = requests.post(url, files=files, data=mD, headers=headerD)
print(response.text)

mD = {
    "idCode": "32684",
    "repositoryType": "onedep-archive",
    "contentType": "em-volume",
    "contentFormat": "map",
    "partNumber": 1,
    "version": 1,
    "copyMode": "native",
    "allowOverWrite": True,
}

url = "http://0.0.0.0:80/file-v1/upload"
awsurl = "http://34.239.121.66:80/file-v1/upload"  # change this to match aws public ipv4

# upload with requests library
with open(cifFilePath, "rb") as ifh:
    files = {"uploadFile": ifh}
    response = requests.post(url, files=files, data=mD, headers=headerD)
print(response.text)

mD = {
    "idCode": "D_001",
    "repositoryType": "onedep-archive",
    "contentType": "model",
    "contentFormat": "pdbx",
    "partNumber": 1,
    "version": 1,
    "copyMode": "native",
    "allowOverWrite": True,
}

url = "http://0.0.0.0:80/file-v1/upload"
awsurl = "http://34.239.121.66:80/file-v1/upload"  # change this to match aws public ipv4

# upload with requests library
with open(cifFilePath, "rb") as ifh:
    files = {"uploadFile": ifh}
    response = requests.post(url, files=files, data=mD, headers=headerD)
print(response.text)
