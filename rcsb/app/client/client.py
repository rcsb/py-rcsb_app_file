import sys
import concurrent.futures
import os
import gzip
from concurrent.futures import ThreadPoolExecutor
import requests
import json
from tqdm.auto import tqdm
import time
import math
import argparse
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.client.ClientUtils import ClientUtils

"""
author James Smith 2023
"""

os.environ["CONFIG_FILE"] = os.path.join(".", "rcsb", "app", "config", "config.yml")
configFilePath = os.environ.get("CONFIG_FILE")
cP = ConfigProvider(configFilePath)
cP.getConfig()
""" modifiable variables
"""
base_url = cP.get("SERVER_HOST_AND_PORT")
maxChunkSize = cP.get("CHUNK_SIZE")
hashType = cP.get("HASH_TYPE")
""" do not alter from here
"""
subject = cP.get("JWT_SUBJECT")
ioU = IoUtils(cP)
cU = ClientUtils()
headerD = {
    "Authorization": "Bearer "
    + JWTAuthToken(configFilePath).createToken({}, subject)
}
RESUMABLE = False
COMPRESS = False
DECOMPRESS = False
OVERWRITE = False
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
    global COMPRESS
    global DECOMPRESS
    global RESUMABLE
    global OVERWRITE
    global cU
    t1 = time.perf_counter()
    readFilePath = mD["filePath"]
    respositoryType = mD["repositoryType"]
    depId = mD["depId"]
    contentType = mD["contentType"]
    milestone = mD["milestone"]
    partNumber = mD["partNumber"]
    contentFormat = mD["contentFormat"]
    version = mD["version"]
    if not readFilePath or not repositoryType or not depId or not contentType or not partNumber or not contentFormat or not version:
        print("error - missing values")
        sys.exit()
    if not os.path.exists(readFilePath):
        sys.exit(f"error - file does not exist: {readFilePath}")
    if milestone.lower() == "none":
        milestone = ""
    # compress, then hash, then upload
    if COMPRESS:
        tempPath = readFilePath + ".gz"
        with open(readFilePath, "rb") as r:
            with gzip.open(tempPath, "wb") as w:
                w.write(r.read())
        readFilePath = tempPath
    # get upload parameters
    response = cU.getUploadParameters(readFilePath, repositoryType, depId, contentType, milestone, partNumber, contentFormat, version, OVERWRITE, RESUMABLE)
    if not response:
        print("error in get upload parameters")
        return
    saveFilePath, chunkIndex, expectedChunks, uploadId, fullTestHash = response
    # upload chunks
    for index in tqdm(range(chunkIndex,expectedChunks), leave=False, total=expectedChunks - chunkIndex, desc=os.path.basename(readFilePath)):
        response = cU.uploadChunk(readFilePath, saveFilePath, index, expectedChunks, uploadId, fullTestHash, DECOMPRESS, OVERWRITE, RESUMABLE)
        if not response:
            print("error in upload chunk")
            break
    return response


def download(downloadFilePath, downloadDict):
    global headerD
    global maxChunkSize
    global COMPRESS
    global DECOMPRESS
    global SEQUENTIAL
    global OVERWRITE
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
            print(f"error - file already exists")
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
    parser.add_argument("-r", "--resumable", action="store_true", help="***** upload resumable sequential chunks *****")
    parser.add_argument("-o", "--overwrite", action="store_true", help="***** overwrite files with same name *****")
    parser.add_argument("-z", "--zip", action="store_true", help="***** zip files prior to upload *****")
    parser.add_argument("-x", "--expand", action="store_true", help="***** unzip files after upload *****")
    args = parser.parse_args()
    uploads = []
    uploadIds = []
    downloads = []
    description()
    if args.resumable:
        RESUMABLE = True
    if args.zip:
        COMPRESS = True
    if args.expand:
        DECOMPRESS = True
    if args.overwrite:
        OVERWRITE = True
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
            uploads.append(
                {
                    "filePath": filePath,
                    "repositoryType": repositoryType,
                    "depId": depId,
                    "contentType": contentType,
                    "milestone": milestone,
                    "partNumber": partNumber,
                    "contentFormat": contentFormat,
                    "version": version,
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
        # upload concurrent files sequential chunks
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(upload, u): u for u in uploads}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for response in results:
                if response:
                    uploadResults.append(response.status_code)
                else:
                    uploadResults.append(None)
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
