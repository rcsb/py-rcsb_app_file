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
import time
import math
import argparse
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.app.file.JWTAuthToken import JWTAuthToken
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils

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
base_url = "http://0.0.0.0:8000"
headerD = {
    "Authorization": "Bearer "
    + JWTAuthToken(cachePath, configFilePath).createToken({}, subject)
}
# headerD = {
#     "Authorization": "Bearer " + cP.get("JWT_DISABLE")
# }
uploadIds = []
uploadResults = []
uploadTexts = []
downloadResults = []
hashType = "MD5"
maxSliceSize = 1024 * 1024 * 8
minSliceSize = 1024  # for development
SLEEP = False  # testing with small files
signature = """
    --------------------------------------------------------
             FILE ACCESS AND DEPOSITION APPLICATION
    --------------------------------------------------------
"""


def asyncUpload(filePath, mD, comp):
    global headerD
    global ioU
    global SLEEP
    responses = []

    uploadId = mD["uploadId"]
    url = os.path.join(base_url, "file-v2", "uploadStatus")
    parameters = {"repositoryType": mD["repositoryType"],
              "idCode": mD["idCode"],
              "contentType": mD["contentType"],
              "milestone": None,
              "partNumber": str(mD["partNumber"]),
              "contentFormat": mD["contentFormat"]
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
            result = eval(result)
            offsetIndex = result["uploadCount"]
            packet_size = min(
                mD["fileSize"] - (mD["sliceIndex"] * mD["sliceSize"]),
                mD["sliceSize"],
            )
            offset = offsetIndex * packet_size
            mD["sliceIndex"] = offsetIndex
            mD["sliceOffset"] = offset
    tmp = io.BytesIO()
    with open(filePath, "rb") as to_upload:
        # print(f'starting from offset {offset} at index {offsetIndex}')
        to_upload.seek(offset)
        url = os.path.join(base_url, "file-v2", "upload")
        for x in tqdm(range(offsetIndex, mD["sliceTotal"]), leave=False, desc=os.path.basename(filePath)):
            packet_size = min(
                mD["fileSize"] - (mD["sliceIndex"] * mD["sliceSize"]),
                mD["sliceSize"],
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
            mD["sliceOffset"] = mD["sliceIndex"] * mD["sliceSize"]
            # text = json.loads(response.text)
            # mD["uploadId"] = text["uploadId"]
            # session = eval(ioU.uploadStatus(mD["uploadId"]))
            # uploadCount = session["uploadCount"]
            # expectedCount = session["expectedCount"]
            # percentage = (uploadCount / expectedCount) * 100
            # print(percentage)
            if SLEEP:
                time.sleep(1)
    return responses  # [responses.pop()]


def download(downloadFilePath, downloadDict):
    global headerD
    global maxSliceSize
    global SLEEP
    url = os.path.join(base_url, "file-v1", "downloadSize")
    fileSize = int(requests.get(url, params=downloadDict, headers=headerD, timeout=None).text)
    chunks = math.ceil(fileSize / maxSliceSize)
    url = os.path.join(base_url, "file-v1", "download", "onedep-archive")
    responseCode = None
    with requests.get(url, params=downloadDict, headers=headerD, timeout=None) as response:
        with open(downloadFilePath, "wb") as ofh:
            for chunk in tqdm(response.iter_content(chunk_size=maxSliceSize), leave=False, total=chunks, desc=os.path.basename(downloadFilePath)):
                ofh.write(chunk)
                if SLEEP:
                    time.sleep(1)
        responseCode = response.status_code
    return responseCode


def description():
    print()
    print(signature)
    print()


if __name__ == "__main__":
    t1 = time.time()
    if len(sys.argv) <= 1:
        description()
        sys.exit('error - please run with -h for instructions')
    parser = argparse.ArgumentParser(description=signature, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', '--compress', nargs=2, help='***** compress input files with gzip *****', metavar=('read-path', 'new-name'))
    parser.add_argument('-t', '--test', action='store_true',
                        help='***** slow motion mode for testing with small files ******')
    parser.add_argument('-u', '--upload', nargs=9, action='append',
                        metavar=('file-path', 'repo-type', 'dep-id', 'content-type', 'milestone', 'part-number', 'content-format', 'version', 'allow-overwrite'),
                        help='***** multiple uploads allowed *****')
    parser.add_argument('-d', '--download', nargs=8, action='append',
                        metavar=('file-path', 'repo-type', 'dep-id', 'content-type', 'milestone', 'part-number', 'content-format', 'version'),
                        help='***** multiple downloads allowed *****')
    args = parser.parse_args()
    if args.test:
        SLEEP = True
        maxSliceSize = minSliceSize
    uploads = []
    uploadIds = []
    downloads = []
    if args.upload or args.download or args.compress:
        description()
    if args.compress:
        arglist = args.compress
        if len(arglist) < 2:
            sys.exit(f'wrong number of args to compress {len(arglist)}')
        filePath = arglist[0]
        newName = arglist[1]
        if not newName.endswith(".gz"):
            newName += ".gz"
        with open(filePath, "rb") as r:
            with open(newName, "wb") as w:
                w.write(gzip.compress(r.read()))
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
            sliceSize = maxSliceSize
            fileSize = os.path.getsize(filePath)
            sliceTotal = 0
            if sliceSize < fileSize:
                sliceTotal = fileSize // sliceSize
                if fileSize % sliceSize:
                    sliceTotal = sliceTotal + 1
            else:
                sliceTotal = 1
            sliceIndex = 0
            sliceOffset = 0
            # url = os.path.join(base_url, "file-v2", "getNewUploadId")
            # response = requests.get(url, headers=headerD, timeout=None)
            # uploadId = json.loads(response.text)
            # if not uploadId:
            #     sys.exit('error - could not get new upload id')
            uploads.append(
                (filePath,
                    {
                        "filePath": filePath,
                        "sliceSize": sliceSize,
                        "sliceIndex": sliceIndex,
                        "sliceOffset": sliceOffset,
                        "sliceTotal": sliceTotal,
                        "fileSize": fileSize,
                        "uploadId": None,
                        "repositoryType": repositoryType,
                        "idCode": depId,
                        "contentType": contentType,
                        "milestone": milestone,
                        "partNumber": part,
                        "contentFormat": contentFormat,
                        "version": version,
                        "copyMode": "native",
                        "allowOverWrite": allowOverwrite,
                        "hashType": hashType,
                        "hashDigest": fullTestHash,
                        "chunkMode": "sequential"
                    }
                )
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
                "idCode": depId,
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
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(asyncUpload, tpl[0], tpl[1], args.compress): tpl for tpl in uploads}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for result in results:
                for response in result:
                    uploadResults.append(response.status_code)
                    res = json.loads(response.text)
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
    # if len(uploadIds) > 0:
    #     url = os.path.join(base_url, "file-v2", "clearSession")
    #     response = requests.post(url, data={"uploadIds": uploadIds}, headers=headerD, timeout=None)
    if len(uploadResults) > 0:
        print(f'upload results {uploadResults}')
    if len(downloadResults) > 0:
        print(f'download results {downloadResults}')
    print("time %.2f seconds" % (time.time() - t1))
