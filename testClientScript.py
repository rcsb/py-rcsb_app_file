import concurrent.futures
import uuid
import os
import io
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import requests
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.utils.io.FileUtil import FileUtil

os.environ["CACHE_PATH"] = os.path.join(
    ".", "rcsb", "app", "tests-file", "test-data", "data"
)
os.environ["CONFIG_FILE"] = os.path.join(".", "rcsb", "app", "config", "config.yml")

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
base_url = "http://0.0.0.0:80"

# create file for download
# select size of file here (in bytes)
nB = 1000
with open(filePath, "wb") as ofh:
    ofh.write(os.urandom(nB))

fU = FileUtil()
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
testHash = hD["hashDigest"]
headerD = {
    "Authorization": "Bearer "
    + JWTAuthToken(cachePath, configFilePath).createToken({}, subject)
}

sessionId = uuid.uuid4().hex


# non-sliced upload


partNumber = 1
copyMode = "native"
allowOverWrite = True

# uploads the file x times
for version in range(1, 9):

    mD = {
        "sliceIndex": 0,
        "sliceOffset": 0,
        "sliceTotal": 1,
        "sessionId": sessionId,
        "idCode": "D_1000000001",
        "repositoryType": "onedep-archive",
        "contentType": "model",
        "contentFormat": "pdbx",
        "partNumber": partNumber,
        "version": str(version),
        "copyMode": copyMode,
        "allowOverWrite": allowOverWrite,
        "hashType": hashType,
        "hashDigest": testHash,
    }

    url = os.path.join(base_url, "file-v2", "upload")

    # upload with requests library
    with open(filePath, "rb") as ifh:
        files = {"uploadFile": ifh}
        response = requests.post(
            url, files=files, data=mD, headers=headerD, timeout=None
        )
    print(response.text)

# download

for version in range(1, 9):

    downloadDict = {
        "idCode": "D_1000000001",
        "repositoryType": "onedep-archive",
        "contentType": "model",
        "contentFormat": "pdbx",
        "partNumber": partNumber,
        "version": str(version),
        "hashType": hashType,
    }
    # set file download path
    fileName = downloadDict["idCode"] + "_" + downloadDict["version"] + ".dat"
    downloadFilePath = os.path.join(
        ".", "test-output", downloadDict["idCode"], fileName
    )
    downloadDirPath = os.path.join(".", "test-output", downloadDict["idCode"])
    downloadName = downloadDict["idCode"] + "_" + "v" + downloadDict["version"]
    FileUtil().mkdir(downloadDirPath)

    url = os.path.join(base_url, "file-v1", "download", "onedep-archive")

    # download with requests library
    response = requests.get(url, params=downloadDict, headers=headerD, timeout=None)

    with open(downloadFilePath, "wb") as ofh:
        ofh.write(response.content)

    print("Download status code:", response.status_code)

# sliced upload

url = os.path.join(base_url, "file-v2", "upload")

partNumber = 1
version = 9
allowOverWrite = True
hashType = "MD5"
hD = CryptUtils().getFileHash(filePath, hashType=hashType)
fullTestHash = hD["hashDigest"]

slices = 4  # make dynamic
file_size = os.path.getsize(filePath)
sliceIndex = 0
slice_size = file_size // slices
sliceTotal = 0
if slice_size < file_size:
    sliceTotal = file_size // slice_size
    if file_size % slice_size:
        sliceTotal = sliceTotal + 1
else:
    sliceTotal = 1
sliceOffset = 0

mD = {
    "sliceIndex": sliceIndex,
    "sliceOffset": sliceOffset,
    "sliceTotal": sliceTotal,
    "sessionId": sessionId,
    "idCode": "D_1000000001",
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

tmp = io.BytesIO()
with open(filePath, "rb") as to_upload:
    for i in range(0, mD["sliceTotal"]):
        packet_size = min(
            file_size - (mD["sliceIndex"] * slice_size),
            slice_size,
        )
        tmp.truncate(packet_size)
        tmp.seek(0)
        tmp.write(to_upload.read(packet_size))
        tmp.seek(0)

        response = requests.post(
            url,
            data=deepcopy(mD),
            headers=headerD,
            files={"uploadFile": tmp},
            timeout=None,
        )
        if response.status_code != 200:
            print(
                f"error - status code {response.status_code} {response.text}...terminating"
            )
            break
        print(f"chunk {i} upload result {response.text}")
        mD["sliceIndex"] += 1
        mD["sliceOffset"] = mD["sliceIndex"] * slice_size

# multifile sliced upload

url = os.path.join(base_url, "file-v2", "upload")

FILES_TO_UPLOAD = [
    "./rcsb/app/tests-file/test-data/data/repository/archive/D_00000000/D_00000000_model_P1.cif.V1",
    "./rcsb/app/tests-file/test-data/data/repository/archive/D_00000000/D_00000000_model_P1.cif.V2",
    "./rcsb/app/tests-file/test-data/data/repository/archive/D_00000000/D_00000000_model_P1.cif.V3",
]


def asyncUpload(filePath):
    global part
    part += 1
    partNumber = part
    version = 1
    allowOverWrite = True
    hashType = "MD5"
    hD = CryptUtils().getFileHash(filePath, hashType=hashType)
    fullTestHash = hD["hashDigest"]
    slices = 4  # make dynamic
    file_size = os.path.getsize(filePath)
    sliceIndex = 0
    slice_size = file_size // slices
    sliceTotal = 0
    if slice_size < file_size:
        sliceTotal = file_size // slice_size
        if file_size % slice_size:
            sliceTotal = sliceTotal + 1
    else:
        sliceTotal = 1
    sliceOffset = 0
    sessionId = uuid.uuid4().hex
    mD = {
        "sliceIndex": sliceIndex,
        "sliceOffset": sliceOffset,
        "sliceTotal": sliceTotal,
        "sessionId": sessionId,
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
    responses = []
    tmp = io.BytesIO()
    with open(filePath, "rb") as to_upload:
        for i in range(0, mD["sliceTotal"]):
            packet_size = min(
                file_size - (mD["sliceIndex"] * slice_size),
                slice_size,
            )
            tmp.truncate(packet_size)
            tmp.seek(0)
            tmp.write(to_upload.read(packet_size))
            tmp.seek(0)
            response = requests.post(
                url,
                data=deepcopy(mD),
                headers=headerD,
                files={"uploadFile": tmp},
                timeout=None,
            )
            if response.status_code != 200:
                print(
                    f"error - status code {response.status_code} {response.text}...terminating"
                )
                break
            responses.append(response)
            mD["sliceIndex"] += 1
            mD["sliceOffset"] = mD["sliceIndex"] * slice_size
    return responses


part = 0
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(asyncUpload, file): file for file in FILES_TO_UPLOAD}
    results = []
    for future in concurrent.futures.as_completed(futures):
        results.append(future.result())
    print("multi-file sliced upload result")
    for result in results:
        print([response.text for response in result])
