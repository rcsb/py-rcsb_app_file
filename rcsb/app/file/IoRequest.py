##
# File: IoRequest.py
# Date: 24-May-2022
# Updates: James Smith 2023
##

__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
from fastapi import APIRouter, Form
from fastapi import Depends
from fastapi import HTTPException
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.IoUtility import IoUtility

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

provider = ConfigProvider()
jwtDisable = bool(provider.get("JWT_DISABLE"))
if not jwtDisable:
    router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["io"])
else:
    router = APIRouter(tags=["io"])


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
    # return status 200 or status 400
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
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="move file fails with %s" % str(exc)
        )


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
    # return status 200 or 400
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
    except Exception as exc:
        logger.error("error - %s", str(exc))
        raise HTTPException(status_code=400, detail="error copying file %s" % str(exc))


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
    # return status 200 or status 400
    try:
        await IoUtility().copyDir(
            repositoryTypeSource,
            depIdSource,
            repositoryTypeTarget,
            depIdTarget,
            overwrite,
        )
    except Exception as exc:
        logger.error("error - %s", str(exc))
        raise HTTPException(status_code=400, detail="copy fails with %s" % str(exc))


@router.post("/compress-dir", status_code=200)
async def compressDir(repositoryType: str = Form(...), depId: str = Form(...)):
    # return status 200 or status 400
    try:
        await IoUtility().compressDir(repositoryType, depId)
    except Exception as exc:
        logger.exception("Failing with %s", str(exc))
        raise HTTPException(
            status_code=400, detail="Directory compression fails with %s" % str(exc)
        )


@router.post("/compress-dir-path")
async def compressDirPath(dirPath: str = Form(...)):
    """Compress directory at given dirPath, as opposed to standard input parameters."""
    # return status 200 or status 400
    try:
        await IoUtility().compressDirPath(dirPath)
    except Exception as exc:
        logger.exception("Failing with %s", str(exc))
        raise HTTPException(
            status_code=400, detail="Directory compression fails with %s" % str(exc)
        )


@router.post("/decompress-dir", status_code=200)
async def decompressDir(repositoryType: str = Form(...), depId: str = Form(...)):
    # return status 200 or status 400
    try:
        await IoUtility().decompressDir(repositoryType, depId)
    except Exception as exc:
        logger.exception("Failing with %s", str(exc))
        raise HTTPException(
            status_code=400, detail="Directory decompression fails with %s" % str(exc)
        )
