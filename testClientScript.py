import asyncio
import uuid
import os
import requests
from rcsb.app.file.IoUtils import IoUtils

os.environ["CACHE_PATH"] = os.path.join(".", "app", "CACHE")
os.environ["CONFIG_FILE"] = os.path.join("rcsb", "app", "config", "config.yml")

from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil

cachePath = os.environ.get("CACHE_PATH")
configFilePath = os.environ.get("CONFIG_FILE")
cP = ConfigProvider(cachePath)
cD = {
    "JWT_SUBJECT": "aTestSubject",
    "JWT_ALGORITHM": "HS256",
    "JWT_SECRET": "aTestSecret",
    "SESSION_DIR_PATH": os.path.join(cachePath, "sessions"),
    "REPOSITORY_DIR_PATH": os.path.join(cachePath, "repository"),
    "SHARED_LOCK_PATH": os.path.join(cachePath, "shared-locks"),
}
cP.getConfig()

repositoryPath = cP.get("REPOSITORY_DIR_PATH")
sessionPath = cP.get("SESSION_DIR_PATH")
subject = cP.get("JWT_SUBJECT")


filePath = "./rcsb/app/tests-file/test-data/testFile.dat"
print(configFilePath + "\n" + cachePath)

# create file for download
# select size of file here (in bytes)
nB = 100
with open(filePath, "wb") as ofh:
    ofh.write(os.urandom(nB))

fU = FileUtil()
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
testHash = hD["hashDigest"]
headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath, configFilePath).createToken({}, subject)}

partNumber = 1
copyMode = "native"
allowOverWrite = True

# uploads the file x times
for version in range(1, 9):

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

    url = "http://0.0.0.0:80/file-v1/upload"

    # upload with requests library
    with open(filePath, "rb") as ifh:
        files = {"uploadFile": ifh}
        response = requests.post(url, files=files, data=mD, headers=headerD)
    print(response.text)

for version in range(1, 9):

    downloadDict = {
        "idCode": "D_00000000",
        "repositoryType": "onedep-archive",
        "contentType": "model",
        "contentFormat": "pdbx",
        "partNumber": partNumber,
        "version": str(version),
        "hashType": hashType,
    }
    # set file download path
    fileName = downloadDict["idCode"] + "_" + downloadDict["version"] + ".dat"
    downloadFilePath = os.path.join(".", "test-output", downloadDict["idCode"], fileName)
    downloadDirPath = os.path.join(".", "test-output", downloadDict["idCode"])
    downloadName = downloadDict["idCode"] + "_" + "v" + downloadDict["version"]
    FileUtil().mkdir(downloadDirPath)

    url = "http://0.0.0.0:80/file-v1/download/onedep-archive"

    # download with requests library
    response = requests.get(url, params=downloadDict, headers=headerD)

    with open(downloadFilePath, "wb") as ofh:
        ofh.write(response.content)

    print(response.status_code)

# sliced upload
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
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
with open(manifestPath, "r", encoding="utf-8") as ifh:
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
            response = requests.post(url, files=files, data=mD, headers=headerD)

partNumber = 1
allowOverWrite = True
version = 1

mD = {
    "sessionId": sessionId,
    "sliceTotal": sliceTotal,
    "idCode": "D_00000000",
    "repositoryType": "onedep-archive",
    "contentType": "model",
    "contentFormat": "pdbx",
    "partNumber": partNumber,
    "version": str(version),
    "copyMode": "native",
    "allowOverWrite": allowOverWrite,
    "hashType": hashType,
    "hashDigest": fullTestHash,
}

url = "http://0.0.0.0:80/file-v1/join-slice"

with open(testFilePath, "rb") as ifh:
    response = requests.post(url, data=mD, headers=headerD)
