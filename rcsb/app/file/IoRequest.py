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
import os
import shutil
from enum import Enum
from fastapi import APIRouter, Form
from fastapi import Depends
from fastapi import Query
from fastapi import HTTPException
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.IoUtility import IoUtility
from rcsb.utils.io.FileUtil import FileUtil


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

provider = ConfigProvider()
jwtDisable = bool(provider.get("JWT_DISABLE"))
if not jwtDisable:
    router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["io"])
else:
    router = APIRouter(tags=["io"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class DirResult(BaseModel):
    dirList: list = Field(
        None,
        title="Directory list",
        description="Directory content list",
        example=["D_0000000001_model_P1.cif.V1", "D_0000000001_model_P1.cif.V2"],
    )


class FileResult(BaseModel):
    success: bool = Field(
        None, title="Success status", description="Success status", example="True"
    )
    fileName: str = Field(
        None,
        title="Stored file name",
        description="Stored file name",
        example="D_0000000001_model_P1.cif.V3",
    )
    version: str = Field(
        None,
        title="Stored file version",
        description="Stored file version",
        example="1,2,3",
    )
    statusCode: int = Field(
        None, title="HTTP status code", description="HTTP status code", example="200"
    )
    statusMessage: str = Field(
        None, title="Status message", description="Status message", example="Success"
    )


class CompressResult(BaseModel):
    success: bool = Field(
        None, title="Success status", description="Success status", example="True"
    )
    dirPath: str = Field(
        None,
        title="Directory path",
        description="Directory path to list",
        example="repository/archive/D_2000000001/",
    )
    compressPath: str = Field(
        None,
        title="Compressed directory path",
        description="Compressed directory path",
        example="repository/archive/D_0000000001.tar.gz",
    )
    statusCode: int = Field(
        None, title="HTTP status code", description="HTTP status code", example="200"
    )
    statusMessage: str = Field(
        None, title="Status message", description="Status message", example="Success"
    )


class PathResult(BaseModel):
    success: bool = Field(
        None, title="Success status", description="Success status", example="True"
    )
    path: str = Field(
        None,
        title="Path",
        description="File or directory path",
        example="repository/archive/D_2000000001/D_2000000001_model_P1.cif.V1",
    )
    statusCode: int = Field(
        None, title="HTTP status code", description="HTTP status code", example="200"
    )
    statusMessage: str = Field(
        None, title="Status message", description="Status message", example="Success"
    )


class CopyFileResult(BaseModel):
    success: bool = Field(
        None, title="Success status", description="Success status", example="True"
    )
    filePathSource: str = Field(
        None,
        title="Source file path",
        description="Stored file name",
        example="D_0000000001_model_P1.cif.V3",
    )
    filePathTarget: str = Field(
        None,
        title="Target file path",
        description="Stored file name",
        example="D_0000000001_model_P1.cif.V3",
    )
    statusCode: int = Field(
        None, title="HTTP status code", description="HTTP status code", example="200"
    )
    statusMessage: str = Field(
        None, title="Status message", description="Status message", example="Success"
    )


@router.post("/copy-file")
async def copyFileRequest(
    repositoryTypeSource: str = Form(),
    depIdSource: str = Form(),
    contentTypeSource: str = Form(),
    milestoneSource: str = Form(""),
    partNumberSource: int = Form(),
    contentFormatSource: str = Form(),
    versionSource: str = Form(),
    repositoryTypeTarget: str = Form(),
    depIdTarget: str = Form(),
    contentTypeTarget: str = Form(),
    milestoneTarget: str = Form(""),
    partNumberTarget: int = Form(),
    contentFormatTarget: str = Form(),
    versionTarget: str = Form(),
):
    try:
        result = await IoUtility().copyFile(
            repositoryTypeSource,
            depIdSource,
            contentTypeSource,
            milestoneSource,
            partNumberSource,
            contentFormatSource,
            versionSource,
            repositoryTypeTarget,
            depIdTarget,
            contentTypeTarget,
            milestoneTarget,
            partNumberTarget,
            contentFormatTarget,
            versionTarget,
        )
    except Exception:
        raise HTTPException(status_code=400, detail="error in copy file")
    return result


@router.post("/move-file")
async def moveFile(
    repositoryTypeSource: str = Form(),
    depIdSource: str = Form(),
    contentTypeSource: str = Form(),
    partNumberSource: int = Form(),
    milestoneSource: str = Form(""),
    contentFormatSource: str = Form(),
    versionSource: str = Form(),
    repositoryTypeTarget: str = Form(),
    depIdTarget: str = Form(),
    contentTypeTarget: str = Form(),
    partNumberTarget: int = Form(),
    milestoneTarget: str = Form(""),
    contentFormatTarget: str = Form(),
    versionTarget: str = Form(),
    overwrite: bool = Form(),
):
    return await IoUtility().moveFile(
        repositoryTypeSource,
        depIdSource,
        contentTypeSource,
        milestoneSource,
        partNumberSource,
        contentFormatSource,
        versionSource,
        repositoryTypeTarget,
        depIdTarget,
        contentTypeTarget,
        milestoneTarget,
        partNumberTarget,
        contentFormatTarget,
        versionTarget,
        overwrite,
    )


@router.post("/copy-dir")
async def copyDir(
    repositoryTypeSource: str = Form(
        title="Source Repository Type",
        description="OneDep repository type of file to copy",
        example="onedep-archive, onedep-deposit",
    ),
    depIdSource: str = Form(
        title="Source ID Code",
        description="Identifier code of file to copy",
        example="D_0000000001",
    ),
    #
    repositoryTypeTarget: str = Form(
        title="Target Repository Type",
        description="OneDep repository type of destination file",
        example="onedep-archive, onedep-deposit",
    ),
    depIdTarget: str = Form(
        title="Target ID Code",
        description="Identifier code of destination file",
        example="D_0000000001",
    ),
):
    logger.info(
        "copy dir request %s %s %s %s",
        repositoryTypeSource,
        depIdSource,
        repositoryTypeTarget,
        depIdTarget,
    )
    try:
        result = await IoUtility().copyDir(
            repositoryTypeSource,
            depIdSource,
            repositoryTypeTarget,
            depIdTarget,
        )
    except Exception as exc:
        logger.error("error - %s", str(exc))
        return None
    return result


@router.post("/compress-dir", response_model=CompressResult)
async def compressDir(
    repositoryType: str = Form(
        title="Repository Type",
        description="OneDep repository type",
        example="onedep-archive, onedep-deposit",
    ),
    depId: str = Form(
        title="ID Code", description="Identifier code", example="D_0000000001"
    ),
):
    """Compress directory of requested depId and repositoryType (using standard input paramaters)."""
    success = False
    compressPath = None
    dirExistsCheck = None
    dirRemovedBool = None
    try:
        fU = FileUtil()
        pathP = PathProvider()
        #
        # Compress directory of requested repositoryType and depId
        dirPath = pathP.getDirPath(repositoryType, depId)
        logger.info(
            "Compressing dirPath %r for repositoryType %r depId %r",
            dirPath,
            repositoryType,
            depId,
        )
        dirExistsCheck = os.path.exists(dirPath)
        if dirExistsCheck:
            compressPath = os.path.abspath(dirPath) + ".tar.gz"
            ok = fU.bundleTarfile(compressPath, [os.path.abspath(dirPath)])
            if ok:
                logger.info(
                    "created compressPath %s from dirPath %s", compressPath, dirPath
                )
                shutil.rmtree(dirPath)
                dirRemovedBool = not os.path.exists(dirPath)
                logger.info("removal status %r for dirPath %s", dirRemovedBool, dirPath)
                if not dirRemovedBool:
                    logger.error(
                        "unable to remove dirPath %s after compression", dirPath
                    )
                success = ok and dirRemovedBool
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(
            status_code=400, detail="Directory compression fails with %s" % str(e)
        )
    #
    if not success:
        if dirExistsCheck is False:
            raise HTTPException(
                status_code=404,
                detail="Requested directory does not exist %s" % dirPath,
            )
        if dirRemovedBool is False:
            raise HTTPException(
                status_code=400,
                detail="Failed to remove directory after compression %s" % dirPath,
            )
        else:
            raise HTTPException(status_code=400, detail="Failed to compress directory")
    else:
        ret = {
            "success": success,
            "dirPath": dirPath,
            "compressPath": compressPath,
            "statusCode": 200,
            "statusMessage": "Directory compressed",
        }

    return ret


@router.post("/compress-dirpath", response_model=CompressResult)
async def compressDirPath(
    dirPath: str = Query(
        title="File directory",
        description="File directory",
        example="/non_standard/directory/",
    ),
):
    """Compress directory at given dirPath, as opposed to standard input paramaters."""
    success = False
    compressPath = None
    dirExistsCheck = None
    dirRemovedBool = None
    try:
        fU = FileUtil()
        #
        logger.info("Compressing dirPath %r", dirPath)
        dirExistsCheck = os.path.exists(dirPath)
        if dirExistsCheck:
            compressPath = os.path.abspath(dirPath) + ".tar.gz"
            ok = fU.bundleTarfile(compressPath, [os.path.abspath(dirPath)])
            if ok:
                logger.info(
                    "created compressPath %s from dirPath %s", compressPath, dirPath
                )
                shutil.rmtree(dirPath)
                dirRemovedBool = not os.path.exists(dirPath)
                logger.info("removal status %r for dirPath %s", dirRemovedBool, dirPath)
                if not dirRemovedBool:
                    logger.error(
                        "unable to remove dirPath %s after compression", dirPath
                    )
                success = ok and dirRemovedBool
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(
            status_code=400, detail="Directory compression fails with %s" % str(e)
        )
    #
    if not success:
        if dirExistsCheck is False:
            raise HTTPException(
                status_code=404,
                detail="Requested directory does not exist %s" % dirPath,
            )
        if dirRemovedBool is False:
            raise HTTPException(
                status_code=400,
                detail="Failed to remove directory after compression %s" % dirPath,
            )
        else:
            raise HTTPException(status_code=400, detail="Failed to compress directory")
    else:
        ret = {
            "success": success,
            "dirPath": dirPath,
            "compressPath": compressPath,
            "statusCode": 200,
            "statusMessage": "Directory compressed",
        }

    return ret
