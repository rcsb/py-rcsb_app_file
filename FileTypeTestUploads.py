#!/usr/bin/python

import requests
import os
import logging

os.environ["CACHE_PATH"] = os.environ.get("CACHE_PATH", os.path.join("rcsb", "app", "data"))
os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join("rcsb", "app", "config", "config.yml"))

from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider

cachePath = os.environ.get("CACHE_PATH")
configFilePath = os.environ.get("CONFIG_FILE")

logger = logging.getLogger()

cifFilePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/example-data.cif"
valReportFilePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/1cbs_validation.pdf"
EMMapFilePath = "/Users/cparker/RCSBWork/py-rcsb_app_file/rcsb/app/tests-file/test-data/emd_32684.map"

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


# # download test
# downloadTimeList = []

# for version in range(1, 10):
#     startTime = time.time()

#     downloadDict = {
#         "idCode": "D_00000001",
#         "repositoryType": "onedep-archive",
#         "contentType": "model",
#         "contentFormat": "cif",
#         "partNumber": partNumber,
#         "version": str(version),
#         "hashType": hashType,
#     }
#     # set file download path
#     downloadFilePath = "./test-output/" + downloadDict["idCode"] + "/" + downloadDict["idCode"] + "_" + downloadDict["version"] + ".dat"
#     downloadDirPath = "./test-output/" + downloadDict["idCode"] + "/"
#     downloadName = downloadDict["idCode"] + "_" + "v" + downloadDict["version"]
#     FileUtil().mkdir(downloadDirPath)

#     headerD = {"Authorization": "Bearer " + JWTAuthToken(cachePath, configFilePath).createToken({}, subject)}
#     url = "http://128.6.159.177:80/file-v1/download/onedep-archive"
#     awsurl = "http://34.239.121.66:80/file-v1/download/onedep-archive"  # change this to match aws public ipv4

#     # upload with requests library
#     # comment out this line to use aws server
#     response = requests.get(url, params=downloadDict, headers=headerD)
#     # comment out this line to use local server
#     # response = requests.get(awsurl, params = downloadDict, headers = headerD)

#     with open(downloadFilePath, "wb") as ofh:
#         ofh.write(response.content)

#     timeSpent = time.time() - startTime
#     logger.info("Completed %s (%.4f seconds)", downloadName, time.time() - startTime)
#     downloadTimeList.append(timeSpent)

# # get average download time
# downloadSum = sum(downloadTimeList)
# downloadAvg = downloadSum / len(downloadTimeList)
# print("\n" "Average download time", downloadAvg, "seconds", "\n"
#       "Size:", os.path.getsize(filePath), "bytes" "\n")


# # sliced upload
# hashType = "MD5"
# hD = CryptUtils().getFileHash(filePath, hashType=hashType)
# fullTestHash = hD["hashDigest"]

# url = "http://128.6.159.177:80/file-v1/upload-slice"
# awsurl = "http://34.239.121.66:80/file-v1/upload-slice"  # change this to match aws public ipv4

# cP = ConfigProvider(cachePath)
# ioU = IoUtils(cP)
# sessionId = uuid.uuid4().hex

# sliceTotal = 4
# loop = asyncio.get_event_loop()
# task = ioU.splitFile(filePath, sliceTotal, "staging" + sessionId, hashType="MD5")
# sP = loop.run_until_complete(task)

# sliceTimeList = []
# sliceIndex = 0
# manifestPath = os.path.join(sP, "MANIFEST")
# with open(manifestPath, "r", encoding="utf-8") as ifh:
#     for line in ifh:
#         testFile = line[:-1]
#         testFilePath = os.path.join(sP, testFile)
#         sliceIndex += 1
#         startTime = time.time()

#         mD = {
#             "sliceIndex": sliceIndex,
#             "sliceTotal": sliceTotal,
#             "sessionId": sessionId,
#             "copyMode": "native",
#             "allowOverWrite": True,
#             "hashType": None,
#             "hashDigest": None,
#         }

#         with open(testFilePath, "rb") as ifh:
#             files = {"uploadFile": ifh}
#             response = requests.post(url, files=files, data=mD, headers=headerD)
#             # response = requests.post(awsurl, files = files, data = mD, headers = headerD)

#         print("Slice", sliceIndex, "in:", str(time.time() - startTime), "seconds")
#         sliceTimeList.append(time.time() - startTime)
# # time taken for each slice to be uploaded
# sliceSum = sum(sliceTimeList)
# sliceAvg = sliceSum / len(sliceTimeList)
# print("\n" "Average slice upload time", sliceAvg, "seconds", "\n"
#       "Size:", os.path.getsize(filePath), "bytes" "\n")


# startTime = time.time()
# partNumber = 1
# allowOverWrite = True
# version = 1

# mD = {
#     "sessionId": sessionId,
#     "sliceTotal": sliceTotal,
#     "idCode": "D_00000000",
#     "repositoryType": "onedep-archive",
#     "contentType": "model",
#     "contentFormat": "cif",
#     "partNumber": partNumber,
#     "version": str(version),
#     "copyMode": "native",
#     "allowOverWrite": allowOverWrite,
#     "hashType": hashType,
#     "hashDigest": fullTestHash,
# }

# url = "http://128.6.159.177:80/file-v1/join-slice"
# awsurl = "http://34.239.121.66:80/file-v1/join-slice"  # change this to match aws public ipv4

# with open(testFilePath, "rb") as ifh:
#     response = requests.post(url, data=mD, headers=headerD)
#     # response = requests.post(awsurl, data = mD, headers = headerD)
# # time taken to join slices
# print("Slices joined in:", str(time.time() - startTime), "seconds\n")
