##
# File: downloadRequest.py
# Date: 11-Aug-2021
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
from enum import Enum

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi.responses import FileResponse
from fastapi.responses import Response
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathUtils import PathUtils
from rcsb.app.file.AwsUtils import AwsUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil

# from pydantic import BaseModel  # pylint: disable=no-name-in-module
# from pydantic import Field


logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["download"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


@router.get("/download/{repositoryType}")
async def download(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Path(None, title="Repository Type", description="Repository type (onedep-archive,onedep-deposit)", example="onedep-archive, onedep-deposit"),
    contentType: str = Query(None, title="Content type", description="Content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Query(None, title="Content format", description="Content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Query(1, title="Content part", description="Content part", example="1,2,3"),
    version: str = Query("1", title="Version string", description="Version number or description", example="1,2,3, latest, previous"),
    hashType: HashType = Query(None, title="Hash type", description="Hash type", example="SHA256"),
):
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    pathU = PathUtils(cP)
    filePath = fileName = mimeType = hashDigest = None
    success = False
    tD = {}
    try:
        filePath = pathU.getVersionedPath(repositoryType, idCode, contentType, partNumber, contentFormat, version)
        success = FileUtil().exists(filePath)

        mimeType = pathU.getMimeType(contentFormat)
        logger.info(
            "success %r repositoryType %r idCode %r contentType %r format %r version %r fileName %r (%r)",
            success,
            repositoryType,
            idCode,
            contentType,
            contentFormat,
            version,
            fileName,
            mimeType,
        )
        # Check hash
        if hashType and success:
            hD = CryptUtils().getFileHash(filePath, hashType.name)
            hashDigest = hD["hashDigest"]
            tD = {"rcsb_hash_type": hashType.name, "rcsb_hexdigest": hashDigest}
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        success = False
    #
    if not success:
        if filePath:
            raise HTTPException(status_code=403, detail="Request file path does not exist %s" % filePath)
        else:
            raise HTTPException(status_code=403, detail="Bad or incomplete path metadata")

    return FileResponse(path=filePath, media_type=mimeType, filename=os.path.basename(filePath), headers=tD)


@router.get("/download-aws")
async def downloadAws(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(None, title="Repository Type", description="Repository type (onedep-archive,onedep-deposit)", example="onedep-archive, onedep-deposit"),
    contentType: str = Query(None, title="Content type", description="Content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Query(None, title="Content format", description="Content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Query(1, title="Content part", description="Content part", example="1,2,3"),
    version: str = Query("1", title="Version string", description="Version number or description", example="1,2,3, latest, previous"),
    hashType: HashType = Query(None, title="Hash type", description="Hash type", example="SHA256")
):
    """Asynchronous download with aioboto3.
    Args:
        key (str): name of file to be retrieved from s3 bucket
    Returns:
        (dict): {"content": Downloaded Data, "status_code": 200|403}
    """
    cachePath = os.environ.get("CACHE_PATH", ".")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)

    pathU = PathUtils(cP)
    awsU = AwsUtils(cP)
    try:
        filePath = pathU.getVersionedPath(repositoryType, idCode, contentType, partNumber, contentFormat, version)
        fileExists = await awsU.checkExists(key=filePath)
        if fileExists:
            downloads3 = await awsU.download(key=filePath)

    except Exception as e:
        logger.exception("Failing with %s", str(e))

    if not fileExists:
        raise HTTPException(status_code=403, detail="Download from S3 failed - File does not exist.")

    # Check hash
    # Will need to add checking to upload when supported by aioboto3
    # https://github.com/terrycain/aioboto3/issues/265
    if hashType:  # and success:
        hD = CryptUtils().getFileHash(filePath, hashType.name)
        hashDigest = hD["hashDigest"]
        tD = {"rcsb_hash_type": hashType.name, "rcsb_hexdigest": hashDigest}
        logger.info("Hash digest %r", tD)

    return Response(content=downloads3, status_code=200)


def isPositiveInteger(tS):
    try:
        return int(tS) >= 0
    except Exception:
        return False
