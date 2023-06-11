import sys
import concurrent.futures
import os
import gzip
from concurrent.futures import ThreadPoolExecutor
import time
import argparse
from rcsb.app.client.ClientUtils import ClientUtils

# author James Smith 2023


def upload(mD):
    # compress, then hash, then upload
    if COMPRESS:
        tempPath = mD["readFilePath"] + ".gz"
        with open(mD["readFilePath"], "rb") as r:
            with gzip.open(tempPath, "wb") as w:
                w.write(r.read())
        mD["filePath"] = tempPath
    # upload
    response = ClientUtils().upload(**mD)
    if not response:
        print("error in upload")
        return None
    elif "status_code" in response:
        return response["status_code"]
    else:
        return None


def download(d):
    response = ClientUtils().download(**d)
    if response and "status_code" in response:
        return response["status_code"]
    else:
        return None


def listDir(r, d):
    response = ClientUtils().listDir(r, d)
    if response and "dirList" in response and "status_code" in response and response["status_code"] == 200:
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
                    "resumable": RESUMABLE
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
                "allowOverwrite": OVERWRITE
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
            futures = {
                executor.submit(download, d): d for d in downloads
            }
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
