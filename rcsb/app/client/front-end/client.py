import sys
import concurrent.futures
import os
import gzip
from concurrent.futures import ThreadPoolExecutor
import time
import argparse
import math
from rcsb.app.file.PathProvider import PathProvider
from tqdm import tqdm
from rcsb.app.client.ClientUtils import ClientUtils
from rcsb.app.file.IoUtility import IoUtility


# author James Smith 2023


def upload(d):
    if not os.path.exists(d["sourceFilePath"]):
        sys.exit(f"error - file does not exist: {d['sourceFilePath']}")
    if d["milestone"].lower() == "none":
        d["milestone"] = ""
    # compress, then hash, then upload
    if COMPRESS:
        tempPath = d["sourceFilePath"] + ".gz"
        with open(d["sourceFilePath"], "rb") as r:
            with gzip.open(tempPath, "wb") as w:
                w.write(r.read())
        d["sourceFilePath"] = tempPath
    # get upload parameters
    response = ClientUtils().getUploadParameters(
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
    # compress (externally), then hash, then upload
    # hash
    hashType = ClientUtils().cP.get("HASH_TYPE")
    fullTestHash = IoUtility().getHashDigest(d["sourceFilePath"], hashType=hashType)
    # compute expected chunks
    fileSize = os.path.getsize(d["sourceFilePath"])
    chunkSize = int(ClientUtils().cP.get("CHUNK_SIZE"))
    expectedChunks = 1
    if chunkSize < fileSize:
        expectedChunks = math.ceil(fileSize / chunkSize)
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
        "decompress": d["decompress"],
        "allowOverwrite": d["allowOverwrite"],
        "resumable": d["resumable"],
    }
    status = None
    for index in tqdm(
        range(chunkIndex, expectedChunks),
        leave=False,
        total=expectedChunks - chunkIndex,
        desc=os.path.basename(d["sourceFilePath"]),
        ascii=True,
    ):
        mD["chunkIndex"] = index
        status = ClientUtils().uploadChunk(d["sourceFilePath"], fileSize, **mD)
        if not status == 200:
            print("error in upload %r" % response)
            break
    return status


def download(d):
    # compute expected chunks
    response = ClientUtils().fileSize(
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
    chunkSize = ClientUtils().cP.get("CHUNK_SIZE")
    expectedChunks = 1
    if chunkSize < fileSize:
        expectedChunks = math.ceil(fileSize / chunkSize)
    # download
    response = ClientUtils().download(
        d["repositoryType"],
        d["depId"],
        d["contentType"],
        d["milestone"],
        d["partNumber"],
        d["contentFormat"],
        d["version"],
        d["downloadFolder"],
        d["allowOverwrite"],
        None,
        None,
        True,
    )
    if response and response["status_code"] == 200:
        status = response["status_code"]
        response = response["response"]
        # write to file
        downloadFilePath = os.path.join(
            d["downloadFolder"],
            PathProvider().getFileName(
                d["depId"],
                d["contentType"],
                d["milestone"],
                d["partNumber"],
                d["contentFormat"],
                d["version"],
            ),
        )
        with open(downloadFilePath, "ab") as ofh:
            for chunk in tqdm(
                response.iter_content(chunk_size=chunkSize),
                leave=False,
                total=expectedChunks,
                desc=os.path.basename(downloadFilePath),
                ascii=True,
            ):
                if chunk:
                    ofh.write(chunk)
        # validate hash
        if (
            "rcsb_hash_type" in response.headers
            and "rcsb_hexdigest" in response.headers
        ):
            rspHashType = response.headers["rcsb_hash_type"]
            rspHashDigest = response.headers["rcsb_hexdigest"]
            hashDigest = IoUtility().getHashDigest(
                downloadFilePath, hashType=rspHashType
            )
            if not hashDigest == rspHashDigest:
                print("error - hash comparison failed")
                return None
        return status
    elif "status_code" in response:
        return response["status_code"]
    return None


def listDir(r, d):
    response = ClientUtils().listDir(r, d)
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
