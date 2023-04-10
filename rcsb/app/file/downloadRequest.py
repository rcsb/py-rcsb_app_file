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
import typing
from enum import Enum
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi.responses import FileResponse, Response
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.CryptUtils import CryptUtils
from rcsb.utils.io.FileUtil import FileUtil

logger = logging.getLogger(__name__)

# not possible to secure an HTML form with a JWT, so no dependencies
router = APIRouter(tags=["download"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


@router.get("/download")
async def download(
    repositoryType: str = Query(None, title="Repository Type", description="Repository type (onedep-archive,onedep-deposit)", example="onedep-archive, onedep-deposit"),
    depId: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    contentType: str = Query(None, title="Content type", description="Content type", example="model, structure-factors, val-report-full"),
    milestone: str = Query("", title="milestone", description="milestone", example="release"),
    partNumber: int = Query(1, title="Content part", description="Content part", example="1,2,3"),
    contentFormat: str = Query(None, title="Content format", description="Content format", example="pdb, pdbx, mtz, pdf"),
    version: str = Query("1", title="Version string", description="Version number or description", example="1,2,3, latest, previous"),
    hashType: HashType = Query(None, title="Hash type", description="Hash type", example="SHA256"),
    chunkSize: typing.Optional[int] = None,
    chunkIndex: typing.Optional[int] = None
):
    cP = ConfigProvider()
    pathU = PathUtils(cP)
    filePath = fileName = mimeType = hashDigest = None
    tD = {}
    try:
        filePath = pathU.getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)
        if not filePath:
            raise HTTPException(status_code=404, detail="Bad or incomplete path metadata")
        if not FileUtil().exists(filePath):
            raise HTTPException(status_code=404, detail="Request file path does not exist %s" % filePath)
        mimeType = pathU.getMimeType(contentFormat)
        logger.info(
            "repositoryType %r depId %r contentType %r format %r version %r fileName %r (%r)",
            repositoryType,
            depId,
            contentType,
            contentFormat,
            version,
            fileName,
            mimeType,
        )
        if hashType:
            hD = CryptUtils().getFileHash(filePath, hashType.name)
            hashDigest = hD["hashDigest"]
            tD = {"rcsb_hash_type": hashType.name, "rcsb_hexdigest": hashDigest}
    except HTTPException as exc:
        logger.exception("Failing with %s", str(exc.detail))
        raise HTTPException(status_code=404, detail=exc.detail)
    if chunkSize is not None and chunkIndex is not None:
        data = None
        try:
            with open(filePath, "rb") as r:
                r.seek(chunkIndex * chunkSize)
                data = r.read(chunkSize)
        except Exception:
            raise HTTPException(status_code=500, detail='error returning chunk')
        return Response(content=data, media_type="application/octet-stream")
    else:
        return FileResponse(path=filePath, media_type=mimeType, filename=os.path.basename(filePath), headers=tD)


@router.get("/downloadSize")
async def downloadSize(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version):
    cP = ConfigProvider()
    pathU = PathUtils(cP)
    filePath = pathU.getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)
    if not filePath or not os.path.exists(filePath):
        raise HTTPException(status_code=404, detail="error - file path does not exist}")
    return os.path.getsize(filePath)
