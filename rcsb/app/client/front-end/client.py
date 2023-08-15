import sys
import concurrent.futures
import os
from concurrent.futures import ThreadPoolExecutor
import time
import argparse
import math
from tqdm import tqdm
from rcsb.app.client.ClientUtility import ClientUtility
from rcsb.app.file.IoUtility import IoUtility
from rcsb.app.file.UploadUtility import UploadUtility


# author James Smith 2023


def upload(d):
    client = ClientUtility()
    COMPRESSION = client.compressionType
    if not os.path.exists(d["sourceFilePath"]):
        sys.exit(f"error - file does not exist: {d['sourceFilePath']}")
    if d["milestone"].lower() == "none":
        d["milestone"] = ""
    # get upload parameters
    response = client.getUploadParameters(
        d["repositoryType"],
        d["depId"],
        d["contentType"],
        d["milestone"],
        d["partNumber"],
        d["contentFormat"],
        d["version"],
        d["allowOverwrite"],
        d["resumable"],
    )
    if not response or response["status_code"] != 200:
        print("error in get upload parameters %r" % response)
        return
    saveFilePath = response["filePath"]
    chunkIndex = response["chunkIndex"]
    uploadId = response["uploadId"]
    # compress, then hash and compute file size parameter, then upload
    decompress = d["decompress"]
    if COMPRESS:
        decompress = True
        print(
            "compressing file %s size %d"
            % (d["sourceFilePath"], os.path.getsize(d["sourceFilePath"]))
        )
        d["sourceFilePath"] = UploadUtility(client.cP).compressFile(
            d["sourceFilePath"], saveFilePath, COMPRESSION
        )
        print(
            "new file name %s file size %d"
            % (d["sourceFilePath"], os.path.getsize(d["sourceFilePath"]))
        )
    # hash
    hashType = client.cP.get("HASH_TYPE")
    fullTestHash = IoUtility().getHashDigest(d["sourceFilePath"], hashType=hashType)
    # compute expected chunks
    fileSize = os.path.getsize(d["sourceFilePath"])
    chunkSize = int(client.cP.get("CHUNK_SIZE"))
    expectedChunks = 1
    if chunkSize < fileSize:
        expectedChunks = math.ceil(fileSize / chunkSize)
    fileExtension = os.path.splitext(d["sourceFilePath"])[-1]
    extractChunk = True
    if decompress or TEST_NO_COMPRESS:
        extractChunk = False
    # upload chunks sequentially
    mD = {
        # chunk parameters
        "chunkSize": chunkSize,
        "chunkIndex": chunkIndex,
        "expectedChunks": expectedChunks,
        # upload file parameters
        "uploadId": uploadId,
        "hashType": hashType,
        "hashDigest": fullTestHash,
        # save file parameters
        "saveFilePath": saveFilePath,
        "fileSize": fileSize,
        "fileExtension": fileExtension,
        "decompress": decompress,
        "allowOverwrite": d["allowOverwrite"],
        "resumable": d["resumable"],
        "extractChunk": extractChunk,
    }
    print(
        "decompress %s extract chunk %s test no compress %s file size %d chunk size %d expected chunks %d"
        % (
            decompress,
            extractChunk,
            TEST_NO_COMPRESS,
            fileSize,
            chunkSize,
            expectedChunks,
        )
    )
    status = None
    for index in tqdm(
        range(chunkIndex, expectedChunks),
        leave=False,
        total=expectedChunks - chunkIndex,
        desc=os.path.basename(d["sourceFilePath"]),
        ascii=False,
    ):
        mD["chunkIndex"] = index
        status = client.uploadChunk(d["sourceFilePath"], **mD)
        if not status == 200:
            print("error in upload %r" % response)
            break
    return status


def download(d):
    client = ClientUtility()
    # compute expected chunks
    response = client.fileSize(
        d["repositoryType"],
        d["depId"],
        d["contentType"],
        d["milestone"],
        d["partNumber"],
        d["contentFormat"],
        d["version"],
    )
    if not response or response["status_code"] != 200:
        print("error computing file size")
        return
    fileSize = int(response["fileSize"])
    chunkSize = client.cP.get("CHUNK_SIZE")
    expectedChunks = 1
    if chunkSize < fileSize:
        expectedChunks = math.ceil(fileSize / chunkSize)

    # download
    statusCode = 0
    for chunkIndex in tqdm(
        range(0, expectedChunks), leave=False, total=expectedChunks, ascii=False
    ):
        response = client.download(
            repositoryType=d["repositoryType"],
            depId=d["depId"],
            contentType=d["contentType"],
            milestone=d["milestone"],
            partNumber=d["partNumber"],
            contentFormat=d["contentFormat"],
            version=d["version"],
            downloadFolder=d["downloadFolder"],
            allowOverwrite=d["allowOverwrite"],
            chunkSize=chunkSize,
            chunkIndex=chunkIndex,
            expectedChunks=expectedChunks,
        )
        if response and response["status_code"] == 200:
            statusCode = response["status_code"]
        elif "status_code" in response:
            print("error - %d" % response["status_code"])
            return response["status_code"]
        else:
            return None
    return statusCode


def listDir(r, d):
    response = ClientUtility().listDir(r, d)
    if (
        response
        and "dirList" in response
        and "status_code" in response
        and response["status_code"] == 200
    ):
        dirList = response["dirList"]
        print(dirList)
        if len(dirList) > 0:
            print("\n")
            print(f"{repoType} {depId}")
            for fi in sorted(dirList):
                print(f"\t{fi}")
            print("\n")
        else:
            print("\nerror - not found\n")


def description():
    print()
    print(signature)
    print()


if __name__ == "__main__":
    t1 = time.perf_counter()

    RESUMABLE = False
    COMPRESS = False
    DECOMPRESS = False
    OVERWRITE = False
    TEST_NO_COMPRESS = False
    uploadIds = []
    uploadResults = []
    uploadTexts = []
    downloadResults = []
    signature = """
        --------------------------------------------------------
                 FILE ACCESS AND DEPOSITION APPLICATION
        --------------------------------------------------------
    """

    if len(sys.argv) <= 1:
        description()
        sys.exit("error - please run with -h for instructions")
    parser = argparse.ArgumentParser(
        description=signature, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-u",
        "--upload",
        nargs=8,
        action="append",
        metavar=(
            "file-path",
            "repo-type",
            "dep-id",
            "content-type",
            "milestone",
            "part-number",
            "content-format",
            "version",
        ),
        help="***** multiple uploads allowed *****",
    )
    parser.add_argument(
        "-d",
        "--download",
        nargs=8,
        action="append",
        metavar=(
            "folder-path",
            "repo-type",
            "dep-id",
            "content-type",
            "milestone",
            "part-number",
            "content-format",
            "version",
        ),
        help="***** multiple downloads allowed *****",
    )
    parser.add_argument(
        "-l",
        "--list",
        nargs=2,
        metavar=("repository-type", "dep-id"),
        help="***** list contents of requested directory *****",
    )
    parser.add_argument(
        "-r",
        "--resumable",
        action="store_true",
        help="***** upload resumable sequential chunks *****",
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="***** overwrite files with same name *****",
    )
    parser.add_argument(
        "-z", "--zip", action="store_true", help="***** zip files prior to upload *****"
    )
    parser.add_argument(
        "-x",
        "--expand",
        action="store_true",
        help="***** unzip files after upload *****",
    )
    parser.add_argument(
        "-t", "--test", action="store_true", help="***** test without compression *****"
    )
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
    if args.test:
        TEST_NO_COMPRESS = True
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
                    "sourceFilePath": filePath,
                    "repositoryType": repositoryType,
                    "depId": depId,
                    "contentType": contentType,
                    "milestone": milestone,
                    "partNumber": partNumber,
                    "contentFormat": contentFormat,
                    "version": version,
                    "decompress": DECOMPRESS,
                    "allowOverwrite": OVERWRITE,
                    "resumable": RESUMABLE,
                }
            )
    if args.download:
        for arglist in args.download:
            if len(arglist) < 8:
                sys.exit(f"error - wrong number of args to download {len(arglist)}")
            downloadFolderPath = arglist[0]
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
                "repositoryType": repositoryType,
                "depId": depId,
                "contentType": contentType,
                "milestone": milestone,
                "partNumber": partNumber,
                "contentFormat": contentFormat,
                "version": str(version),
                "downloadFolder": downloadFolderPath,
                "allowOverwrite": OVERWRITE,
            }
            downloads.append(downloadDict)
    if len(uploads) > 0:
        # upload concurrent files sequential chunks
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(upload, u): u for u in uploads}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for status_code in results:
                if status_code:
                    uploadResults.append(status_code)
                else:
                    uploadResults.append(None)
    if len(downloads) > 0:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(download, d): d for d in downloads}
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
        listDir(repoType, depId)

    print("time %.2f seconds" % (time.perf_counter() - t1))
