import asyncio
import copy
import subprocess
import sys
import concurrent.futures
import os
import io
import gzip
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import requests
import json
from tqdm.auto import tqdm
from tqdm.asyncio import trange
import time
import math
import argparse
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils

"""
author James Smith 2023
"""

""" modifiable variables
"""
base_url = "http://0.0.0.0:8000"
maxChunkSize = 1024 * 1024 * 8
hashType = "MD5"

""" do not alter from here
"""

os.environ["CONFIG_FILE"] = os.path.join(".", "rcsb", "app", "config", "config.yml")
configFilePath = os.environ.get("CONFIG_FILE")
cP = ConfigProvider(configFilePath)
cP.getConfig()
subject = cP.get("JWT_SUBJECT")
ioU = IoUtils(cP)
headerD = {
    "Authorization": "Bearer "
    + JWTAuthToken(configFilePath).createToken({}, subject)
}
SEQUENTIAL = False
RESUMABLE = False
COMPRESS = False
DECOMPRESS = False
OVERWRITE = False
EMAIL_ADDRESS = None
uploadIds = []
uploadResults = []
uploadTexts = []
downloadResults = []
signature = """
    --------------------------------------------------------
             FILE ACCESS AND DEPOSITION APPLICATION
    --------------------------------------------------------
"""


def upload(mD):
    global headerD
    global ioU
    global maxChunkSize
    global COMPRESS
    global DECOMPRESS
    global SEQUENTIAL
    global RESUMABLE
    global OVERWRITE
    global EMAIL_ADDRESS
    if not SEQUENTIAL and not RESUMABLE:
        # upload as one file
        response = None
        with open(mD["filePath"], "rb") as to_upload:
            url = os.path.join(base_url, "file-v2", "upload")
            response = requests.post(
                url,
                data=deepcopy(mD),
                headers=headerD,
                files={"uploadFile": to_upload},
                stream=True,
                timeout=None,
            )
        if response.status_code != 200:
            print(
                f"error - status code {response.status_code} {response.text}"
            )
        return [response]
    elif SEQUENTIAL:
        # sequential chunks
        readFilePath = mD["filePath"]
        url = os.path.join(base_url, "file-v2", "getSaveFilePath")
        parameters = {"repositoryType": mD["repositoryType"],
                      "depId": mD["depId"],
                      "contentType": mD["contentType"],
                      "milestone": mD["milestone"],
                      "partNumber": str(mD["partNumber"]),
                      "contentFormat": mD["contentFormat"],
                      "version": mD["version"],
                      "allowOverwrite": mD["allowOverwrite"]
                      }
        response = requests.get(
            url,
            params=parameters,
            headers=headerD,
            timeout=None
        )
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                mD["filePath"] = result["path"]
        url = os.path.join(base_url, "file-v2", "getNewUploadId")
        response = requests.get(
            url,
            headers=headerD,
            timeout=None
        )
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                mD["uploadId"] = result["id"]
        # chunk file and upload
        offset = 0
        uploadCount = 0
        responses = []
        tmp = io.BytesIO()
        with open(readFilePath, "rb") as to_upload:
            to_upload.seek(offset)
            url = os.path.join(base_url, "file-v2", "sequentialUpload")
            for x in tqdm(range(uploadCount, mD["expectedChunks"]), leave=False, desc=os.path.basename(mD["filePath"])):
                packet_size = min(
                    int(mD["fileSize"]) - (int(mD["chunkIndex"]) * int(mD["chunkSize"])),
                    int(mD["chunkSize"]),
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
                    stream=True,
                    timeout=None,
                )
                if response.status_code != 200:
                    print(
                        f"error - status code {response.status_code} {response.text}...terminating"
                    )
                    break
                responses.append(response)
                mD["chunkIndex"] += 1
                mD["chunkOffset"] = mD["chunkIndex"] * mD["chunkSize"]
        return responses
    elif RESUMABLE:
        # resumable sequential chunk upload
        responses = []
        uploadId = mD["uploadId"]
        url = os.path.join(base_url, "file-v2", "uploadStatus")
        parameters = {"repositoryType": mD["repositoryType"],
                  "depId": mD["depId"],
                  "contentType": mD["contentType"],
                  "milestone": mD["milestone"],
                  "partNumber": str(mD["partNumber"]),
                  "contentFormat": mD["contentFormat"],
                  "hashDigest": mD["hashDigest"]
                  }
        response = requests.get(
            url,
            params=parameters,
            headers=headerD,
            timeout=None
        )
        uploadCount = 0
        offset = 0
        if response.status_code == 200:
            result = json.loads(response.text)
            if result:
                if not isinstance(result, dict):
                    result = eval(result)
                uploadCount = int(result["uploadCount"])
                packet_size = min(
                    int(mD["fileSize"]) - ( int(mD["chunkIndex"]) * int(mD["chunkSize"]) ),
                    int(mD["chunkSize"]),
                )
                offset = uploadCount * packet_size
                mD["chunkIndex"] = uploadCount
                mD["chunkOffset"] = offset
        # chunk file and upload
        tmp = io.BytesIO()
        with open(mD["filePath"], "rb") as to_upload:
            to_upload.seek(offset)
            url = os.path.join(base_url, "file-v2", "resumableUpload")
            for x in tqdm(range(uploadCount, mD["expectedChunks"]), leave=False, desc=os.path.basename(mD["filePath"])):
                packet_size = min(
                    int(mD["fileSize"]) - ( int(mD["chunkIndex"]) * int(mD["chunkSize"]) ),
                    int(mD["chunkSize"]),
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
                    stream=True,
                    timeout=None,
                )
                if response.status_code != 200:
                    print(
                        f"error - status code {response.status_code} {response.text}...terminating"
                    )
                    break
                responses.append(response)
                mD["chunkIndex"] += 1
                mD["chunkOffset"] = mD["chunkIndex"] * mD["chunkSize"]
        return responses

def download(downloadFilePath, downloadDict):
    global headerD
    global maxChunkSize
    global COMPRESS
    global DECOMPRESS
    global SEQUENTIAL
    global OVERWRITE
    global EMAIL_ADDRESS
    url = os.path.join(base_url, "file-v1", "downloadSize")
    fileSize = requests.get(url, params=downloadDict, headers=headerD, timeout=None).text
    if not fileSize.isnumeric():
        print(f"error - no response for {downloadDict}")
        return None
    fileSize = int(fileSize)
    chunkSize = maxChunkSize
    chunks = math.ceil(fileSize / maxChunkSize)
    url = os.path.join(base_url, "file-v1", "download")
    responseCode = None
    count = 0
    if os.path.exists(downloadFilePath):
        if downloadDict["allowOverwrite"].lower() == "true":
            os.remove(downloadFilePath)
        else:
            print(f'error - file already exists')
            return None
    with requests.get(url, params=downloadDict, headers=headerD, timeout=None, stream=True) as response:
        with open(downloadFilePath, "ab") as ofh:
            for chunk in tqdm(response.iter_content(chunk_size=chunkSize), leave=False, total=chunks, desc=os.path.basename(downloadFilePath)):
                if chunk:
                    ofh.write(chunk)
                count += 1
        responseCode = response.status_code
        rspHashType = response.headers["rcsb_hash_type"]
        rspHashDigest = response.headers["rcsb_hexdigest"]
        thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
        if not thD["hashDigest"] == rspHashDigest:
            print("error - hash comparison failed")
            sys.exit()
    return responseCode


def description():
    print()
    print(signature)
    print()


if __name__ == "__main__":
    t1 = time.perf_counter()
    if len(sys.argv) <= 1:
        description()
        sys.exit("error - please run with -h for instructions")
    parser = argparse.ArgumentParser(description=signature, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-u", "--upload", nargs=8, action="append",
                        metavar=("file-path", "repo-type", "dep-id", "content-type", "milestone", "part-number", "content-format", "version"),
                        help="***** multiple uploads allowed *****")
    parser.add_argument("-d", "--download", nargs=8, action="append",
                        metavar=("file-path", "repo-type", "dep-id", "content-type", "milestone", "part-number", "content-format", "version"),
                        help="***** multiple downloads allowed *****")
    parser.add_argument("-l", "--list", nargs=2, metavar=("repository-type", "dep-id"), help="***** list contents of requested directory *****")
    parser.add_argument("-e", "--email", nargs=1, metavar=("email_address"), help="***** set email address *****")
    parser.add_argument("-s", "--sequential", action="store_true", help="***** upload sequential chunks *****")
    parser.add_argument("-r", "--resumable", action="store_true", help="***** upload resumable sequential chunks *****")
    parser.add_argument("-o", "--overwrite", action="store_true", help="***** overwrite files with same name *****")
    parser.add_argument("-z", "--zip", action="store_true", help="***** zip files prior to upload *****")
    parser.add_argument("-x", "--expand", action="store_true", help="***** unzip files after upload *****")
    args = parser.parse_args()
    uploads = []
    uploadIds = []
    downloads = []
    description()
    if args.sequential:
        SEQUENTIAL = True
    if args.resumable:
        RESUMABLE = True
    if SEQUENTIAL and RESUMABLE:
        sys.exit('error - mututally incompatible options')
    if args.zip:
        COMPRESS = True
    if args.expand:
        DECOMPRESS = True
    if args.overwrite:
        OVERWRITE = True
    if args.email:
        EMAIL_ADDRESS = args.email[0]
    if args.upload:
        for arglist in args.upload:
            if len(arglist) < 8:
                sys.exit(f"error - wrong number of args to upload: {len(arglist)}")
            filePath = arglist[0]
            if not os.path.exists(filePath):
                sys.exit(f"error - file does not exist: {filePath}")
            repositoryType = arglist[1]
            depId = arglist[2]
            contentType = arglist[3]
            milestone = arglist[4]
            if milestone.lower() == "none":
                milestone = ""
            partNumber = arglist[5]
            contentFormat = arglist[6]
            version = arglist[7]
            allowOverwrite = OVERWRITE
            # compress, then hash, then upload
            if COMPRESS:
                tempPath = filePath + ".gz"
                with open(filePath, "rb") as r:
                    with gzip.open(tempPath, "wb") as w:
                        w.write(r.read())
                filePath = tempPath
            # hash
            hD = CryptUtils().getFileHash(filePath, hashType=hashType)
            fullTestHash = hD["hashDigest"]
            chunkSize = maxChunkSize
            fileSize = os.path.getsize(filePath)
            expectedChunks = 0
            if chunkSize < fileSize:
                expectedChunks = fileSize // chunkSize
                if fileSize % chunkSize:
                    expectedChunks = expectedChunks + 1
            else:
                expectedChunks = 1
            chunkIndex = 0
            chunkOffset = 0
            copyMode = "native"
            if DECOMPRESS:
                copyMode = "gzip_decompress"
            # upload complete file
            if not SEQUENTIAL and not RESUMABLE:
                uploads.append({
                    # upload file parameters
                    "filePath": filePath,
                    "uploadId": None,
                    "fileSize": fileSize,
                    "hashType": hashType,
                    "hashDigest": fullTestHash,
                    # save file parameters
                    "repositoryType": repositoryType,
                    "depId": depId,
                    "contentType": contentType,
                    "milestone": milestone,
                    "partNumber": partNumber,
                    "contentFormat": contentFormat,
                    "version": version,
                    "copyMode": copyMode,
                    "allowOverwrite": allowOverwrite
                })
            # upload chunks
            elif RESUMABLE or SEQUENTIAL:
                uploads.append(
                    {
                        # upload file parameters
                        "filePath": filePath,
                        "uploadId": None,
                        "fileSize": fileSize,
                        "hashType": hashType,
                        "hashDigest": fullTestHash,
                        # chunk parameters
                        "chunkSize": chunkSize,
                        "chunkIndex": chunkIndex,
                        "chunkOffset": chunkOffset,
                        "expectedChunks": expectedChunks,
                        # save file parameters
                        "repositoryType": repositoryType,
                        "depId": depId,
                        "contentType": contentType,
                        "milestone": milestone,
                        "partNumber": partNumber,
                        "contentFormat": contentFormat,
                        "version": version,
                        "copyMode": copyMode,  # whether file is a zip file
                        "allowOverwrite": allowOverwrite,
                        "emailAddress": EMAIL_ADDRESS
                    }
                )
    if args.download:
        for arglist in args.download:
            if len(arglist) < 8:
                sys.exit(f"error - wrong number of args to download {len(arglist)}")
            downloadFilePath = arglist[0]
            repositoryType = arglist[1]
            depId = arglist[2]
            contentType = arglist[3]
            milestone = arglist[4]
            if milestone.lower() == "none":
                milestone = ""
            partNumber = arglist[5]
            contentFormat = arglist[6]
            version = arglist[7]
            allowOverwrite = OVERWRITE
            downloadDict = {
                "depId": depId,
                "repositoryType": repositoryType,
                "contentType": contentType,
                "contentFormat": contentFormat,
                "partNumber": partNumber,
                "version": str(version),
                "hashType": hashType,
                "milestone": milestone,
                "allowOverwrite": allowOverwrite
            }
            downloads.append((downloadFilePath, downloadDict))
    if len(uploads) > 0:
        # upload concurrent files sequential chunks or no chunks
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(upload, u): u for u in uploads}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for result in results:
                for response in result:
                    uploadResults.append(response.status_code)
                    res = json.loads(response.text)
                    if res and res.get("uploadId"):
                        uploadIds.append(res["uploadId"])
                    uploadTexts.append(res)
    if len(downloads) > 0:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(download, tpl[0], tpl[1]): tpl for tpl in downloads}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for status_code in results:
                downloadResults.append(status_code)
    if len(uploadResults) > 0:
        print(f"upload results {uploadResults}")
    if len(downloadResults) > 0:
        print(f"download results {downloadResults}")
    if args.list:
        arglist = args.list
        if not len(arglist) == 2:
            sys.exit("error - list takes two args")
        repoType = arglist[0]
        depId = arglist[1]
        parameters = {
            "depId": depId,
            "repositoryType": repoType
        }
        url = os.path.join(base_url, "file-v1", "list-dir")
        responseCode = None
        dirList = None
        with requests.get(url, params=parameters, headers=headerD, timeout=None) as response:
            responseCode = response.status_code
            if responseCode == 200:
                resp = response.text
                if resp:
                    if not isinstance(resp, dict):
                        resp = json.loads(resp)
                    dirList = resp["dirList"]
        print(f"response {responseCode}")
        if responseCode == 200:
            for fi in sorted(dirList):
                print(f"\t{fi}")
    print("time %.2f seconds" % (time.perf_counter() - t1))
