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
from fastapi import HTTPException
from fastapi import Query
from fastapi.responses import FileResponse
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil

logger = logging.getLogger(__name__)

router = APIRouter(tags=["download"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


@router.get("/downloadSize")
async def downloadSize(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version):  # hashType=None):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    pathU = PathUtils(cP)
    filePath = pathU.getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)
    return os.path.getsize(filePath)


@router.get("/download")
async def download(
    repositoryType: str = Query(None, title="Repository Type", description="Repository type (onedep-archive,onedep-deposit)", example="onedep-archive, onedep-deposit"),
    depId: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    contentType: str = Query(None, title="Content type", description="Content type", example="model, structure-factors, val-report-full"),
    milestone: str = Query("", title="milestone", description="milestone", example="release"),
    partNumber: int = Query(1, title="Content part", description="Content part", example="1,2,3"),
    contentFormat: str = Query(None, title="Content format", description="Content format", example="pdb, pdbx, mtz, pdf"),
    version: str = Query("1", title="Version string", description="Version number or description", example="1,2,3, latest, previous"),
    hashType: HashType = Query(None, title="Hash type", description="Hash type", example="SHA256")
):
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(configFilePath)
    pathU = PathUtils(cP)
    filePath = fileName = mimeType = hashDigest = None
    success = False
    tD = {}
    try:
        filePath = pathU.getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)
        success = FileUtil().exists(filePath)
        mimeType = pathU.getMimeType(contentFormat)
        logger.info(
            "success %r repositoryType %r depId %r contentType %r format %r version %r fileName %r (%r)",
            success,
            repositoryType,
            depId,
            contentType,
            contentFormat,
            version,
            fileName,
            mimeType,
        )
        if hashType and success:
            hD = CryptUtils().getFileHash(filePath, hashType.name)
            hashDigest = hD["hashDigest"]
            tD = {"rcsb_hash_type": hashType.name, "rcsb_hexdigest": hashDigest}
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        success = False
    if not success:
        if filePath:
            raise HTTPException(status_code=403, detail="Request file path does not exist %s" % filePath)
        else:
            raise HTTPException(status_code=403, detail="Bad or incomplete path metadata")
    return FileResponse(path=filePath, media_type=mimeType, filename=os.path.basename(filePath), headers=tD)


def isPositiveInteger(tS):
    try:
        return int(tS) >= 0
    except Exception:
        return False
