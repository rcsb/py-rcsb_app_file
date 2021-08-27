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
import os.path
from enum import Enum

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Path
from fastapi import Query
from fastapi.responses import FileResponse
from rcsb.app.file.FileSystemUtils import FileSystemUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil

# from pydantic import BaseModel  # pylint: disable=no-name-in-module
# from pydantic import Field


logger = logging.getLogger(__name__)

router = APIRouter()


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


@router.get("/download/{repository}", dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def download(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_00000000"),
    version: str = Query("1", title="Version string", description="Version number or description", example="1,2,3, latest, previous"),
    contentType: str = Query(None, title="Content type", description="Content type", example="model, sf, val-report-full"),
    contentFormat: str = Query(None, title="Content format", description="Content format", example="cif, xml, json, txt"),
    partNumber: int = Query(1, title="Content part", description="Content part", example="1,2,3"),
    hashType: HashType = Query(None, title="Hash type", description="Hash type", example="SHA256"),
    repository: str = Path(None, title="Repository", description="Repository (onedep-archive,onedep-deposit)", example="onedep-archive, onedep-deposit"),
):
    fsU = FileSystemUtils()
    filePath = fileName = mimeType = hashDigest = None
    success = False
    try:
        repoPath = fsU.getRepositoryPath(repository)
        #
        if isPositiveInteger(version):
            fileName = f"{idCode}_{contentType}_P{partNumber}.{contentFormat}.V{version}"
            filePath = os.path.join(repoPath, idCode, fileName)
        else:
            filePath = fsU.getVersionedPath(version, repoPath, idCode, contentType, partNumber, contentFormat)
            fileName = os.path.basename(filePath)
        success = FileUtil().exists(filePath)
        if not success:
            logger.error("bad path %r", filePath)
        mimeType = fsU.getMimeType(contentFormat)
        logger.debug("success %r idCode %r contentType %r format %r version %r fileName %r (%r)", success, idCode, contentType, contentFormat, version, fileName, mimeType)

        # Check hash
        if hashType:
            hD = CryptUtils().getFileHash(filePath, hashType.name)
            hashDigest = hD["hashDigest"]
            tD = {"rcsb_hash_type": hashType.name, "rcsb_hexdigest": hashDigest}
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        success = False
    return FileResponse(path=filePath, media_type=mimeType, filename=fileName, headers=tD)


def isPositiveInteger(tS):
    try:
        return int(tS) >= 0
    except Exception:
        return False
