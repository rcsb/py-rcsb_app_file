##
# File: ioRequest.py
# Date: 24-May-2022
# Updates: James Smith 2023
##

__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
from fastapi import APIRouter, Form, HTTPException, Query, Depends
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.IoUtility import IoUtility

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

provider = ConfigProvider()
bypassAuthorization = bool(provider.get("BYPASS_AUTHORIZATION"))
if not bypassAuthorization:
    router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["io"])
else:
    router = APIRouter(tags=["io"])

# functions
# get hash, move file, copy file, copy dir, compress dir, compress dir path, decompress dir


@router.get("/get-hash")
async def getHash(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: str = Query(default=""),
    partNumber: int = Query(...),
    contentFormat: str = Query(...),
    version: str = Query(...),
):
    filePath = PathProvider().getVersionedPath(
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    )
    if not filePath:
        raise HTTPException(status_code=404, detail="error - could not form file path")
    hashDigest = IoUtility().getHashDigest(filePath)
    if not hashDigest:
        raise HTTPException(
            status_code=421, detail="error - could not form hash digest"
        )
    return {"hashDigest": hashDigest}


@router.post("/move-file", status_code=200)
async def moveFile(
    repositoryTypeSource: str = Form(),
    depIdSource: str = Form(),
    contentTypeSource: str = Form(),
    partNumberSource: int = Form(),
    milestoneSource: str = Form(""),
    contentFormatSource: str = Form(),
    versionSource: str = Form(),
    #
    repositoryTypeTarget: str = Form(),
    depIdTarget: str = Form(),
    contentTypeTarget: str = Form(),
    partNumberTarget: int = Form(),
    milestoneTarget: str = Form(""),
    contentFormatTarget: str = Form(),
    versionTarget: str = Form(),
    #
    overwrite: bool = Form(default=False),
):
    # return status
    try:
        await IoUtility().moveFile(
            repositoryTypeSource,
            depIdSource,
            contentTypeSource,
            milestoneSource,
            partNumberSource,
            contentFormatSource,
            versionSource,
            #
            repositoryTypeTarget,
            depIdTarget,
            contentTypeTarget,
            milestoneTarget,
            partNumberTarget,
            contentFormatTarget,
            versionTarget,
            #
            overwrite,
        )
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/copy-file", status_code=200)
async def copyFile(
    repositoryTypeSource: str = Form(...),
    depIdSource: str = Form(...),
    contentTypeSource: str = Form(...),
    milestoneSource: str = Form(default=""),
    partNumberSource: int = Form(...),
    contentFormatSource: str = Form(...),
    versionSource: str = Form(...),
    #
    repositoryTypeTarget: str = Form(...),
    depIdTarget: str = Form(...),
    contentTypeTarget: str = Form(...),
    milestoneTarget: str = Form(default=""),
    partNumberTarget: int = Form(...),
    contentFormatTarget: str = Form(...),
    versionTarget: str = Form(...),
    #
    overwrite: bool = Form(default=False),
):
    # return status
    try:
        await IoUtility().copyFile(
            repositoryTypeSource,
            depIdSource,
            contentTypeSource,
            milestoneSource,
            partNumberSource,
            contentFormatSource,
            versionSource,
            #
            repositoryTypeTarget,
            depIdTarget,
            contentTypeTarget,
            milestoneTarget,
            partNumberTarget,
            contentFormatTarget,
            versionTarget,
            #
            overwrite,
        )
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/copy-dir", status_code=200)
async def copyDir(
    repositoryTypeSource: str = Form(...),
    depIdSource: str = Form(...),
    #
    repositoryTypeTarget: str = Form(...),
    depIdTarget: str = Form(...),
    #
    overwrite: bool = Form(default=False),
):
    # return status
    try:
        await IoUtility().copyDir(
            repositoryTypeSource,
            depIdSource,
            repositoryTypeTarget,
            depIdTarget,
            overwrite,
        )
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/make-dirs", status_code=200)
async def makeDirs(repositoryType: str = Form(...), depId: str = Form(...)):
    # return status
    try:
        await IoUtility().makeDirs(repositoryType, depId)
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/make-dir", status_code=200)
async def makeDir(repositoryType: str = Form(...), depId: str = Form(...)):
    # return status
    try:
        await IoUtility().makeDir(repositoryType, depId)
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/compress-dir", status_code=200)
async def compressDir(repositoryType: str = Form(...), depId: str = Form(...)):
    # return status
    try:
        await IoUtility().compressDir(repositoryType, depId)
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/compress-dir-path", status_code=200)
async def compressDirPath(dirPath: str = Form(...)):
    """Compress directory at given dirPath, as opposed to standard input parameters."""
    # return status
    try:
        await IoUtility().compressDirPath(dirPath)
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/decompress-dir", status_code=200)
async def decompressDir(repositoryType: str = Form(...), depId: str = Form(...)):
    # return status
    await IoUtility().decompressDir(repositoryType, depId)
