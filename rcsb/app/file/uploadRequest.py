##
# File: uploadRequest.py
# Date: 27-Oct-2022
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
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import UploadFile
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.AwsUtils import AwsUtils
from rcsb.app.file.PathUtils import PathUtils

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["upload"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class UploadResult(BaseModel):
    sliceIndex: int = Field(None, title="Slice index", description="Slice index", example=1)
    sliceCount: int = Field(None, title="Slice count", description="Number of slices currently uploaded (if applicable)", example=2)
    success: bool = Field(None, title="Success status", description="Success status", example=True)
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example=200)
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


# Add Endpoints:
# - getUploadStatus

@router.post("/upload", response_model=UploadResult)
async def upload(
    uploadFile: UploadFile = File(...),
    sliceIndex: int = Form(1, title="Index of the current chunk", description="Index of the current chunk", example="1"),
    sliceOffset: int = Form(0, title="Chunk byte offset", description="Chunk byte offset", example="1024"),
    sliceTotal: int = Form(1, title="Total number of chunks in the session", description="Total number of chunks in the session", example="5"),
    sessionId: str = Form(None, title="Session identifier", description="Unique identifier for the current session", example="9fe2c4e93f654fdbb24c02b15259716c"),
    idCode: str = Form(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Form(None, title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    contentType: str = Form(None, title="Content Type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Form(None, title="Content format", description="Content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Form(None, title="Part Number", description="OneDep part number", example="1"),
    version: str = Form(None, title="Version", description="OneDep version number of descriptor", example="1, 2, latest, next"),
    copyMode: str = Form("native", title="Copy mode", description="Copy mode", example="shell|native|gzip_decompress"),
    allowOverWrite: bool = Form(False, title="Allow overwrite of existing files", description="Allow overwrite of existing files", example="False"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
):
    # logging.warning(f'upload {idCode} {version} slice {sliceIndex}')
    fn = None
    ct = None
    try:
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        #
        logger.debug("idCode %r hash %r hashType %r", idCode, hashDigest, hashType)
        #
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("uploadFile %s (%r)", fn, ct)
        #
        if fn.endswith(".gz") or ct == "application/gzip":
            copyMode = "gzip_decompress"
        #
        logger.debug("hashType.name %r hashDigest %r", hashType, hashDigest)
        ioU = IoUtils(cP)
        ret = await ioU.storeUpload(
            uploadFile.file,
            sliceIndex,
            sliceOffset,
            sliceTotal,
            sessionId,
            idCode,
            repositoryType,
            contentType,
            contentFormat,
            partNumber,
            version,
            copyMode=copyMode,
            allowOverWrite=allowOverWrite,
            hashType=hashType,
            hashDigest=hashDigest
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {"success": False, "statusCode": 400, "statusMessage": "Upload fails with %s" % str(e)}
    #
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    # logging.warning(f'returning {str(ret)}')
    return ret


