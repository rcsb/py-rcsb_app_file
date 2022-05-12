#!/usr/bin/python                                                                                                                                 

import asyncio
import uuid
import requests
import os
import logging
import time
import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rcsb.app.file.IoUtils import IoUtils

os.environ["CACHE_PATH"] = "/mnt/vdb1/fileAppData"

from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.app.file.main import app

logger = logging.getLogger()

filePath = "/opt/py-rcsb_app_file/rcsb/app/tests-file/test-data/testFile.dat"
fileName = "/opt/py-rcsb_app_file/testFile.txt"
cachePath = "/mnt/vdb1/fileAppData"
configFilePath = "./rcsb/app/config/config.yml"

# create file for download                                                                                                                        
# select size of file here (in bytes)                                                                                                             
nB = 1000000
with open(fileName, "wb") as ofh:
    ofh.write(os.urandom(nB))


cP = ConfigProvider(cachePath, configFilePath)
cP.getConfig()

repositoryPath = cP.get("REPOSITORY_DIR_PATH")
sessionPath = cP.get("SESSION_DIR_PATH")
subject = cP.get("JWT_SUBJECT")
fU = FileUtil()
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
testHash = hD["hashDigest"]
headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath, configFilePath).createToken({}, subject)}

partNumber = 1
copyMode = "native"
allowOverWrite = True
version = 1
fileName = "/opt/py-rcsb_app_file/testFile.txt"

mD = {
    "idCode": "D_00000000",
    "repositoryType": "onedep-archive",
    "contentType": "model",
    "contentFormat": "pdbx",
    "partNumber": partNumber,
    "version": str(version),
    "copyMode": copyMode,
    "allowOverWrite": allowOverWrite,
    "hashType": hashType,
    "hashDigest": testHash
}

#client = TestClient(app)                                                                                                                         

#s3_client = boto3.client('s3')                                                                                                                   

#response = s3_client.generate_presigned_post(Bucket="rcsb-file-api", Key="testFile.txt", ExpiresIn=10)                                           

#print(response)                                                                                                                                  

with open(fileName, "r") as ifh:
    files = {"files": ifh}
    r = requests.post("http://0.0.0.0:8000/file-v1/upload-aws", files=files)
#r = requests.post(response['url'], data=response['fields'], files=files)                                                                         


#with open(fileName, "rb") as ifh:                                                                                                                
#    files = {"uploadFile": ifh}                                                                                                                  
#   r = requests.post("http://128.6.159.177/file-v1/upload-aws", files=files, data=mD, headers=headerD)                                           
#files = {"UploadFile": open(fileName, "rb")}                                                                                                     

#with open(fileName, "rb") as ifh:                                                                                                                
#        files = {"uploadFile": ifh}                                                                                                              
#        response = requests.post("http://128.6.159.177:80/file-v1/upload", files=files, data=mD, headers=headerD)                                

#print(response.text)                                                                                                                             


print(r.text)
print(r.status_code)