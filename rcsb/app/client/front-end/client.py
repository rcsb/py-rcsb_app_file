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

RESUMABLE = False
COMPRESS_FILE = False
DECOMPRESS_FILE = False
OVERWRITE = False
COMPRESS_CHUNKS = False
NO_CHUNKS = False
signature = """
    --------------------------------------------------------
             FILE ACCESS AND DEPOSITION APPLICATION
    --------------------------------------------------------
"""
repoType = None
depId = None


def upload(d):
    client = ClientUtility()
    COMPRESSION = client.compressionType
    if not os.path.exists(d["sourceFilePath"]):
        sys.exit(f"error - file does not exist: {d['sourceFilePath']}")
    if d["milestone"].lower() == "none":
        d["milestone"] = ""

    if NO_CHUNKS:
        extractChunk = True
        if d["decompress"] or not COMPRESS_CHUNKS:
            extractChunk = False
        fileExtension = os.path.splitext(d["sourceFilePath"])[-1]
        response = client.upload(
            d["sourceFilePath"],
            d["repositoryType"],
            d["depId"],
            d["contentType"],
            d["milestone"],
            d["partNumber"],
            d["contentFormat"],
            d["version"],
            d["decompress"],
            fileExtension,
            d["allowOverwrite"],
            d["resumable"],
            extractChunk,
        )
        if response:
            status = response["status_code"]
            if not status == 200:
                print("error in upload %d" % status)
            else:
                return status
        else:
            print("error in upload - no response")
        return None

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
        return None
    saveFilePath = response["filePath"]
    chunkIndex = response["chunkIndex"]
    uploadId = response["uploadId"]
    # compress, then hash and compute file size parameter, then upload
    decompress = d["decompress"]
    if COMPRESS_FILE:
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
    if decompress or not COMPRESS_CHUNKS:
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
        "decompress %s extract chunk %s compress chunks %s file size %d chunk size %d expected chunks %d"
        % (
            decompress,
            extractChunk,
            COMPRESS_CHUNKS,
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
        if status != 200:
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
        return None
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


def copy(d):
    client = ClientUtility()
    statusCode = 0
    response = client.copyFile(**d)
    if response and response["status_code"] == 200:
        statusCode = response["status_code"]
    elif "status_code" in response:
        print("error - %d" % response["status_code"])
        return response["status_code"]
    else:
        return None
    return statusCode


def move(d):
    client = ClientUtility()
    statusCode = 0
    response = client.moveFile(**d)
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
    COMPRESS_FILE = False
    DECOMPRESS_FILE = False
    OVERWRITE = False
    COMPRESS_CHUNKS = False
    NO_CHUNKS = False
    uploadIds = []
    uploadResults = []
    uploadTexts = []
    downloadResults = []
    copyResults = []
    moveResults = []
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
        "-c",
        "--copy",
        nargs=14,
        action="append",
        metavar=(
            "source-repo-type",
            "source-dep-id",
            "source-content-type",
            "source-milestone",
            "source-part",
            "source-content-format",
            "source-version",
            "target-repo-type",
            "target-dep-id",
            "target-content-type",
            "target-milestone",
            "target-part",
            "target-content-format",
            "target-version",
        ),
        help="***** copy file *****",
    )
    parser.add_argument(
        "-m",
        "--move",
        nargs=14,
        action="append",
        metavar=(
            "source-repo-type",
            "source-dep-id",
            "source-content-type",
            "source-milestone",
            "source-part",
            "source-content-format",
            "source-version",
            "target-repo-type",
            "target-dep-id",
            "target-content-type",
            "target-milestone",
            "target-part",
            "target-content-format",
            "target-version",
        ),
        help="***** move file *****",
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
        "-z",
        "--zip",
        action="store_true",
        help="***** zip complete file prior to upload *****",
    )
    parser.add_argument(
        "-x",
        "--expand",
        action="store_true",
        help="***** unzip complete file after upload *****",
    )
    parser.add_argument(
        "-g", "--gzip", action="store_true", help="***** compress chunks *****"
    )
    parser.add_argument(
        "-n", "--nochunks", action="store_true", help="***** no chunking *****"
    )
    args = parser.parse_args()
    uploads = []
    uploadIds = []
    downloads = []
    copies = []
    moves = []
    description()
    if args.resumable:
        RESUMABLE = True
    if args.zip:
        COMPRESS_FILE = True
    if args.expand:
        DECOMPRESS_FILE = True
    if args.overwrite:
        OVERWRITE = True
    if args.gzip:
        COMPRESS_CHUNKS = True
    if args.nochunks:
        NO_CHUNKS = True
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
                    "decompress": DECOMPRESS_FILE,
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
    if args.copy:
        for arglist in args.copy:
            if len(arglist) < 8:
                sys.exit(f"error - wrong number of args to upload: {len(arglist)}")
            sourceRepositoryType = arglist[0]
            sourceDepId = arglist[1]
            sourceContentType = arglist[2]
            sourceMilestone = arglist[3]
            if sourceMilestone.lower() == "none":
                sourceMilestone = ""
            sourcePartNumber = arglist[4]
            sourceContentFormat = arglist[5]
            sourceVersion = arglist[6]
            targetRepositoryType = arglist[7]
            targetDepId = arglist[8]
            targetContentType = arglist[9]
            targetMilestone = arglist[10]
            if targetMilestone.lower() == "none":
                targetMilestone = ""
            targetPartNumber = arglist[11]
            targetContentFormat = arglist[12]
            targetVersion = arglist[13]
            copies.append(
                {
                    "repositoryTypeSource": sourceRepositoryType,
                    "depIdSource": sourceDepId,
                    "contentTypeSource": sourceContentType,
                    "milestoneSource": sourceMilestone,
                    "partNumberSource": sourcePartNumber,
                    "contentFormatSource": sourceContentFormat,
                    "versionSource": sourceVersion,
                    "repositoryTypeTarget": targetRepositoryType,
                    "depIdTarget": targetDepId,
                    "contentTypeTarget": targetContentType,
                    "milestoneTarget": targetMilestone,
                    "partNumberTarget": targetPartNumber,
                    "contentFormatTarget": targetContentFormat,
                    "versionTarget": targetVersion,
                    "overwrite": OVERWRITE,
                }
            )
    if args.move:
        for arglist in args.move:
            if len(arglist) < 8:
                sys.exit(f"error - wrong number of args to upload: {len(arglist)}")
            sourceRepositoryType = arglist[0]
            sourceDepId = arglist[1]
            sourceContentType = arglist[2]
            sourceMilestone = arglist[3]
            if sourceMilestone.lower() == "none":
                sourceMilestone = ""
            sourcePartNumber = arglist[4]
            sourceContentFormat = arglist[5]
            sourceVersion = arglist[6]
            targetRepositoryType = arglist[7]
            targetDepId = arglist[8]
            targetContentType = arglist[9]
            targetMilestone = arglist[10]
            if targetMilestone.lower() == "none":
                targetMilestone = ""
            targetPartNumber = arglist[11]
            targetContentFormat = arglist[12]
            targetVersion = arglist[13]
            moves.append(
                {
                    "repositoryTypeSource": sourceRepositoryType,
                    "depIdSource": sourceDepId,
                    "contentTypeSource": sourceContentType,
                    "milestoneSource": sourceMilestone,
                    "partNumberSource": sourcePartNumber,
                    "contentFormatSource": sourceContentFormat,
                    "versionSource": sourceVersion,
                    "repositoryTypeTarget": targetRepositoryType,
                    "depIdTarget": targetDepId,
                    "contentTypeTarget": targetContentType,
                    "milestoneTarget": targetMilestone,
                    "partNumberTarget": targetPartNumber,
                    "contentFormatTarget": targetContentFormat,
                    "versionTarget": targetVersion,
                    "overwrite": OVERWRITE,
                }
            )
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
    if len(copies) > 0:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(copy, c): c for c in copies}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for status_code in results:
                copyResults.append(status_code)
    if len(moves) > 0:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(move, m): m for m in moves}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
            for status_code in results:
                moveResults.append(status_code)
    if len(uploadResults) > 0:
        print(f"upload results {uploadResults}")
    if len(downloadResults) > 0:
        print(f"download results {downloadResults}")
    if len(copyResults) > 0:
        print(f"copy results {copyResults}")
    if len(moveResults) > 0:
        print(f"move results {moveResults}")
    if args.list:
        arglist = args.list
        if not len(arglist) == 2:
            sys.exit("error - list takes two args")
        repoType = arglist[0]
        depId = arglist[1]
        listDir(repoType, depId)

    print("time %.2f seconds" % (time.perf_counter() - t1))
