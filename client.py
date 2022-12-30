import asyncio
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

""" modifiable variables
"""
base_url = "http://0.0.0.0:8000"
maxChunkSize = 1024 * 1024 * 8

""" test configuration variables
    set sleep = false for slow motion testing of small files with min chunk size
    modifiable from command line args
"""
SLEEP = False
minChunkSize = 1024

""" do not alter from here
"""
os.environ["CACHE_PATH"] = os.path.join(
    ".", "rcsb", "app", "tests-file", "test-data", "data"
)
os.environ["CONFIG_FILE"] = os.path.join(".", "rcsb", "app", "config", "config.yml")
cachePath = os.environ.get("CACHE_PATH")
configFilePath = os.environ.get("CONFIG_FILE")
cP = ConfigProvider(cachePath)
cP.getConfig()
subject = cP.get("JWT_SUBJECT")
ioU = IoUtils(cP)
headerD = {
    "Authorization": "Bearer "
    + JWTAuthToken(cachePath, configFilePath).createToken({}, subject)
}
hashType = "MD5"
COMPRESS = False

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
    global SLEEP
    global COMPRESS

    responses = []
    # test for resumed upload
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
    offsetIndex = 0
    offset = 0
    if response.status_code == 200:
        result = json.loads(response.text)
        if result:
            if not isinstance(result, dict):
                result = eval(result)
            offsetIndex = int(result["uploadCount"])
            packet_size = min(
                int(mD["fileSize"]) - ( int(mD["chunkIndex"]) * int(mD["chunkSize"]) ),
                int(mD["chunkSize"]),
            )
            offset = offsetIndex * packet_size
            mD["chunkIndex"] = offsetIndex
            mD["chunkOffset"] = offset
    # chunk file and upload
    tmp = io.BytesIO()
    with open(mD["filePath"], "rb") as to_upload:
        to_upload.seek(offset)
        url = os.path.join(base_url, "file-v2", "upload")
        for x in tqdm(range(offsetIndex, mD["expectedChunks"]), leave=False, desc=os.path.basename(mD["filePath"])):
            packet_size = min(
                int(mD["fileSize"]) - ( int(mD["chunkIndex"]) * int(mD["chunkSize"]) ),
                int(mD["chunkSize"]),
            )
            tmp.truncate(packet_size)
            tmp.seek(0)
            if COMPRESS:
                tmp.write(gzip.compress(to_upload.read(packet_size)))
            else:
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
            mD["chunkIndex"] += 1
            mD["chunkOffset"] = mD["chunkIndex"] * mD["chunkSize"]
            # text = json.loads(response.text)
            # mD["uploadId"] = text["uploadId"]
            # session = eval(ioU.uploadStatus(mD["uploadId"]))
            # uploadCount = session["uploadCount"]
            # expectedCount = session["expectedCount"]
            # percentage = (uploadCount / expectedCount) * 100
            # print(percentage)
            if SLEEP:
                time.sleep(1)
    return responses


async def asyncFiles(uploads):
    tasks = [asyncFile(upload) for upload in uploads]
    return await asyncio.gather(*tasks)
    # not async
    # results = []
    # async for x in trange(len(uploads)):
    #     results.append(await asyncFile(uploads[x]))
    # return results


async def asyncFile(mD):
    global headerD
    global ioU
    responses = []
    # test for resumed upload
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
    chunksSaved = "0" * mD["expectedChunks"]
    if response.status_code == 200:
        result = json.loads(response.text)
        if result:
            result = eval(result)
            chunksSaved = result["chunksSaved"]
    tasks = []
    for index in range(0, len(chunksSaved)):
        if chunksSaved[index] == "0":
            tasks.append(asyncChunk(index, mD))
    responses = await asyncio.gather(*tasks)
    results = []
    for response in responses:
        status = response.status_code
        text = json.loads(response.text)
        uploadId = text["uploadId"]
        results.append(status)
    return results


async def asyncChunk(index, mD):
    filePath = mD["filePath"]
    offset = index * mD["chunkSize"]
    mD["chunkIndex"] = index
    response = None
    # chunk file and upload
    tmp = io.BytesIO()
    with open(filePath, "rb") as to_upload:
        to_upload.seek(offset)
        url = os.path.join(base_url, "file-v2", "upload")
        packet_size = min(
            mD["fileSize"] - (mD["chunkIndex"] * mD["chunkSize"]),
            mD["chunkSize"],
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
        else:
            mD["chunkIndex"] += 1
            mD["chunkOffset"] = mD["chunkIndex"] * mD["chunkSize"]
    return response


def download(downloadFilePath, downloadDict):
    global headerD
    global maxChunkSize
    global minChunkSize
    global SLEEP
    url = os.path.join(base_url, "file-v1", "downloadSize")
    fileSize = requests.get(url, params=downloadDict, headers=headerD, timeout=None).text
    if not fileSize.isnumeric():
        print(f'error - no response for {downloadDict}')
        return None
    fileSize = int(fileSize)
    chunkSize = maxChunkSize
    if SLEEP:
        chunkSize = minChunkSize
    chunks = math.ceil(fileSize / maxChunkSize)
    url = os.path.join(base_url, "file-v1", "download", downloadDict["repositoryType"])
    responseCode = None
    count = 0
    if os.path.exists(downloadFilePath):
        os.remove(downloadFilePath)
    with requests.get(url, params=downloadDict, headers=headerD, timeout=None, stream=True) as response:
        with open(downloadFilePath, "ab") as ofh:
            for chunk in tqdm(response.iter_content(chunk_size=chunkSize), leave=False, total=chunks, desc=os.path.basename(downloadFilePath)):
                if chunk:
                    ofh.write(chunk)
                # print(f'wrote chunk {count} of {chunks} of size {chunkSize} for {downloadFilePath}')
                count += 1
                if SLEEP:
                    time.sleep(1)
        responseCode = response.status_code
        rspHashType = response.headers["rcsb_hash_type"]
        rspHashDigest = response.headers["rcsb_hexdigest"]
        thD = CryptUtils().getFileHash(downloadFilePath, hashType=rspHashType)
        if not thD["hashDigest"] == rspHashDigest:
            print('error - hash comparison failed')
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
        sys.exit('error - please run with -h for instructions')
    parser = argparse.ArgumentParser(description=signature, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', '--upload', nargs=9, action='append',
                        metavar=('file-path', 'repo-type', 'dep-id', 'content-type', 'milestone', 'part-number', 'content-format', 'version', 'allow-overwrite'),
                        help='***** multiple uploads allowed *****')
    parser.add_argument('-d', '--download', nargs=8, action='append',
                        metavar=('file-path', 'repo-type', 'dep-id', 'content-type', 'milestone', 'part-number', 'content-format', 'version'),
                        help='***** multiple downloads allowed *****')
    parser.add_argument('-l', '--list', nargs=2, metavar=('repository-type', 'dep-id'), help='***** list contents of requested directory *****')
    parser.add_argument('-p', '--parallel', action='store_true', help='***** upload parallel chunks *****')
    parser.add_argument('-c', '--compress', action='store_true', help='***** compress files or chunks prior to upload')
    # parser.add_argument('-c', '--compress', nargs=2, help='***** compress with gzip and output new file *****', metavar=('read-path', 'new-name'))
    parser.add_argument('-t', '--test', action='store_true',
                        help='***** slow motion mode for testing with small files (sequential chunks only) ******')
    args = parser.parse_args()
    if args.test:
        SLEEP = True
        maxChunkSize = minChunkSize
    uploads = []
    uploadIds = []
    downloads = []
    if args.upload or args.download or args.compress:
        description()
    if args.compress:
        COMPRESS = True
        # arglist = args.compress
        # if len(arglist) < 2:
        #     sys.exit(f'wrong number of args to compress {len(arglist)}')
        # filePath = arglist[0]
        # newName = arglist[1]
        # if not newName.endswith(".gz"):
        #     newName += ".gz"
        # with open(filePath, "rb") as r:
        #     with open(newName, "wb") as w:
        #         w.write(gzip.compress(r.read()))
    if args.upload:
        for arglist in args.upload:
            if len(arglist) < 9:
                sys.exit(f'error - wrong number of args to upload: {len(arglist)}')
            filePath = arglist[0]
            if not os.path.exists(filePath):
                sys.exit(f'error - file does not exist: {filePath}')
            repositoryType = arglist[1]
            depId = arglist[2]
            contentType = arglist[3]
            milestone = arglist[4]
            if milestone.lower() == "none":
                milestone = ""
            part = arglist[5]
            contentFormat = arglist[6]
            version = arglist[7]
            allowOverwrite = arglist[8]
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
            chunkMode = "sequential"
            if args.parallel:
                chunkMode = "async"
            copyMode = "native"
            if COMPRESS:
                copyMode = "gzip_decompress"
            # url = os.path.join(base_url, "file-v2", "getNewUploadId")
            # response = requests.get(url, headers=headerD, timeout=None)
            # uploadId = json.loads(response.text)
            # if not uploadId:
            #     sys.exit('error - could not get new upload id')
            uploads.append(
                    {
                        # upload file parameters
                        "filePath": filePath,
                        "uploadId": None,
                        "fileSize": fileSize,
                        "hashType": hashType,
                        "hashDigest": fullTestHash,
                        "copyMode": copyMode, # whether file is a zip file
                        # chunk parameters
                        "chunkSize": chunkSize,
                        "chunkIndex": chunkIndex,
                        "chunkOffset": chunkOffset,
                        "expectedChunks": expectedChunks,
                        "chunkMode": chunkMode,
                        # save file parameters
                        "repositoryType": repositoryType,
                        "depId": depId,
                        "contentType": contentType,
                        "milestone": milestone,
                        "partNumber": part,
                        "contentFormat": contentFormat,
                        "version": version,
                        "allowOverWrite": allowOverwrite
                    }
            )
    if args.download:
        for arglist in args.download:
            if len(arglist) < 8:
                sys.exit(f'error - wrong number of args to download {len(arglist)}')
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
            downloadDict = {
                "depId": depId,
                "repositoryType": repositoryType,
                "contentType": contentType,
                "contentFormat": contentFormat,
                "partNumber": partNumber,
                "version": str(version),
                "hashType": hashType,
                "milestone": milestone
            }
            downloads.append((downloadFilePath, downloadDict))
    if len(uploads) > 0:
        if uploads[0]["chunkMode"] == "async":
            # upload concurrent files concurrent chunks
            uploadResults = asyncio.run(asyncFiles(uploads))
        elif uploads[0]["chunkMode"] == "sequential":
            # upload concurrent files sequential chunks
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(upload, u): u for u in uploads}
                results = []
                for future in concurrent.futures.as_completed(futures):
                    results.append(future.result())
                for result in results:
                    for response in result:
                        uploadResults.append(response.status_code)
                        res = json.loads(response.text)
                        uploadIds.append(res["uploadId"])
                        uploadTexts.append(res)
        else:
            sys.exit("error - unknown chunk mode")
    if len(downloads) > 0:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(download, tpl[0], tpl[1]): tpl for tpl in downloads}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for status_code in results:
                downloadResults.append(status_code)
    if len(uploadResults) > 0:
        print(f'upload results {uploadResults}')
    if len(downloadResults) > 0:
        print(f'download results {downloadResults}')
    if args.list:
        arglist = args.list
        if not len(arglist) == 2:
            sys.exit('error - list takes two args')
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
        print(f'response {responseCode}')
        if responseCode == 200:
            for fi in sorted(dirList):
                print(f'\t{fi}')
    print("time %.2f seconds" % (time.perf_counter() - t1))
