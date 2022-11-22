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
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.FileLock import FileLock

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["upload"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class UploadResult(BaseModel):
    sliceIndex: int = Field(
        None, title="Slice index", description="Slice index", example=1
    )
    sliceCount: int = Field(
        None,
        title="Slice count",
        description="Number of slices currently uploaded (if applicable)",
        example=2,
    )
    success: bool = Field(
        None, title="Success status", description="Success status", example=True
    )
    statusCode: int = Field(
        None, title="HTTP status code", description="HTTP status code", example=200
    )
    statusMessage: str = Field(
        None, title="Status message", description="Status message", example="Success"
    )


# Add Endpoints:
# - getUploadStatus


@router.post("/uploadStatus")
async def getUploadStatus(uploadId) -> list:
    return await IoUtils.uploadStatus(uploadId)


# not yet implemented, have no session id per user
@router.post("/serverStatus")
async def getServerStatus(sessionId: int) -> list:
    return await IoUtils.serverStatus(sessionId)


@router.post("/findUploadId")
async def findUploadId(repositoryType: str = Form(...), idCode: str = Form(...), partNumber: int = Form(...), contentType: str = Form(...), contentFormat: str = Form(...)) -> int:
    return await IoUtils.findUploadId(repositoryType, idCode, partNumber, contentType, contentFormat)


# create new upload id
@router.post("/getNewUploadId")
async def getNewUploadId():
    return await IoUtils.getNewUploadId()


@router.post("/clearSession")
async def clearSession(uploadIds: list = Form(...)):
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    return await ioU.clearSession(uploadIds)

@router.post("/clearKv")
async def clearKv():
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)
    ioU = IoUtils(cP)
    return await ioU.clearKv()

@router.post("/upload")  # , response_model=UploadResult)
async def upload(
    uploadFile: UploadFile = File(...),
    sliceSize: int = Form(
        None,
        title="Size of one slice",
        description="Size of one slice",
        example="1024"
    ),
    sliceIndex: int = Form(
        1,
        title="Index of the current chunk",
        description="Index of the current chunk",
        example="1",
    ),
    sliceOffset: int = Form(
        0, title="Chunk byte offset", description="Chunk byte offset", example="1024"
    ),
    sliceTotal: int = Form(
        1,
        title="Total number of chunks in the session",
        description="Total number of chunks in the session",
        example="5",
    ),
    fileSize: int = Form(
        None,
        title="Length of entire file",
        description="Length of entire file",
        example="4194304"
    ),
    uploadId: str = Form(
        None,
        title="Session identifier",
        description="Unique identifier for the current session",
        example="9fe2c4e93f654fdbb24c02b15259716c",
    ),
    idCode: str = Form(
        None, title="ID Code", description="Identifier code", example="D_0000000001"
    ),
    repositoryType: str = Form(
        None,
        title="Repository Type",
        description="OneDep repository type",
        example="onedep-archive, onedep-deposit",
    ),
    contentType: str = Form(
        None,
        title="Content Type",
        description="OneDep content type",
        example="model, structure-factors, val-report-full",
    ),
    contentFormat: str = Form(
        None,
        title="Content format",
        description="Content format",
        example="pdb, pdbx, mtz, pdf",
    ),
    partNumber: int = Form(
        None, title="Part Number", description="OneDep part number", example="1"
    ),
    version: str = Form(
        None,
        title="Version",
        description="OneDep version number of descriptor",
        example="1, 2, latest, next",
    ),
    copyMode: str = Form(
        "native",
        title="Copy mode",
        description="Copy mode",
        example="shell|native|gzip_decompress",
    ),
    allowOverWrite: bool = Form(
        False,
        title="Allow overwrite of existing files",
        description="Allow overwrite of existing files",
        example="False",
    ),
    hashType: HashType = Form(
        None, title="Hash type", description="Hash type", example="SHA256"
    ),
    hashDigest: str = Form(
        None,
        title="Hash digest",
        description="Hash digest",
        example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'",
    ),
):
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
        """ get upload id
            avoid problem of new id for each chunk
            user should pass None for upload id
            could return id and have user set parameter for subsequent chunk uploads
            however, not possible for concurrent chunks, which could not use return values
            to enable possible concurrent chunk functions in future, all uploads treated as resumed uploads
            upload id parameter is always None, then found from KV if it already exists
        """
        if uploadId is None:  # and sliceIndex == 0 and sliceOffset == 0:
            # check for resumed upload
            # if find resumed upload then set uid = previous uid? (would get redundant chunk error)
            # lock so that even for concurrent chunks the first chunk will write an upload id that subsequent chunks will use
            pathU = PathUtils(cP)
            lockPath = pathU.getFileLockPath(idCode, contentType, partNumber, contentFormat)
            with FileLock(lockPath):
                uploadId = await ioU.getResumedUpload(repositoryType, idCode, contentType, partNumber, contentFormat, version)
                if not uploadId:
                    # logging.warning("generating new id at slice %s", sliceIndex)
                    uploadId = await ioU.getNewUploadId()#repositoryType, idCode, contentType, partNumber, contentFormat, version)
                else:
                    pass
                    # logging.warning("found previous id at slice %s", sliceIndex)
        ret = await ioU.storeUpload(
            uploadFile.file,
            sliceIndex,
            sliceOffset,
            sliceTotal,
            uploadId,
            idCode,
            repositoryType,
            contentType,
            contentFormat,
            partNumber,
            version,
            copyMode=copyMode,
            allowOverWrite=allowOverWrite,
            hashType=hashType,
            hashDigest=hashDigest,
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {
            "success": False,
            "statusCode": 400,
            "statusMessage": f"Upload fails with {str(e)}",
        }
    #
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret
