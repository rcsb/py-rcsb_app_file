##
# File: IoRequest.py
# Date: 24-May-2022
#
# To Do:
# - Add Endpoints:
#   - getFileHash
# - Remove default setting for select params where appropriate (here and in tests), to make them required
# - Make 'repositoryType' an enumerated parameter (not possible with Query() parameter)
# - better way to determine latest version? (use subroutine)
##
__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import shutil
import typing
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
logger = logging.getLogger(__name__)


provider = ConfigProvider()
jwtDisable = bool(provider.get("JWT_DISABLE"))
if not jwtDisable:
    router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["status"])
else:
    router = APIRouter(tags=["status"])


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


@router.get("/file-path")
async def getFilePath(
    repositoryType: str = Query(...),
    depId: str = Query(...),
    contentType: str = Query(...),
    milestone: typing.Optional[str] = Query(default="next"),
    partNumber: int = Query(...),
    contentFormat: str = Query(...),
    version: str = Query(default="next"),
):
    result = None
    try:
        result = PathProvider().getFilePath(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
    except HTTPException as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"content": result}


@router.get("/list-dir")  # , response_model=DirResult)
async def listDir(
    repositoryType: str = Query(
        title="Repository Type",
        description="OneDep repository type",
        example="onedep-archive, onedep-deposit",
    ),
    depId: str = Query(
        title="ID Code", description="Identifier code", example="D_0000000001"
    ),
):
    return await IoUtility().listDir(repositoryType, depId)


@router.get("/file-exists", response_model=FileResult)
async def fileExists(
    repositoryType: str = Query(
        title="Repository Type",
        description="OneDep repository type",
        example="onedep-archive, onedep-deposit",
    ),
    depId: str = Query(
        title="ID Code", description="Identifier code", example="D_0000000001"
    ),
    contentType: str = Query(
        title="Content type",
        description="OneDep content type",
        example="model, structure-factors, val-report-full",
    ),
    milestone: str = Query(
        "", title="milestone", description="milestone", example="release"
    ),
    partNumber: int = Query(
        1, title="Content part", description="OneDep part number", example="1,2,3"
    ),
    contentFormat: str = Query(
        title="Content format",
        description="OneDep content format",
        example="pdb, pdbx, mtz, pdf",
    ),
    version: str = Query(
        "latest",
        title="Version string",
        description="OneDep version number or description",
        example="1,2,3, latest, previous",
    )
):
    """Check if a file exists provided standard file parameters."""
    success = False
    try:
        # fU = FileUtil()
        pathP = PathProvider()
        #
        logger.info(
            "Checking repositoryType %r depId %r contentType %r milestone %r format %r version %r",
            repositoryType,
            depId,
            contentType,
            milestone,
            contentFormat,
            version,
        )
        filePath = pathP.getVersionedPath(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
        logger.info("Checking filePath %r", filePath)
        fileName = os.path.basename(filePath)
        # fileName = fU.getFileName(filePath)
        success = os.path.exists(filePath)
        logger.info("success %r fileName %r filepath %r", success, fileName, filePath)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(
            status_code=400, detail="File checking fails with %s" % str(e)
        )
    #
    if not success:
        if filePath:
            raise HTTPException(
                status_code=404, detail="Request file path does not exist %s" % filePath
            )
        else:
            raise HTTPException(
                status_code=400, detail="Bad or incomplete path metadata"
            )
    else:
        ret = {
            "success": success,
            "fileName": fileName,
            "version": version,
            "statusCode": 200,
            "statusMessage": "File exists",
        }

    return ret


@router.get("/dir-exists", response_model=PathResult)
async def dirExists(
    repositoryType: str = Query(
        title="Repository Type",
        description="OneDep repository type",
        example="onedep-archive, onedep-deposit",
    ),
    depId: str = Query(
        title="ID Code", description="Identifier code", example="D_0000000001"
    ),
):
    """Check if a file exists provided standard directory parameters."""
    success = False
    try:
        pathP = PathProvider()
        #
        logger.info("Checking repositoryType %r depId %r", repositoryType, depId)
        dirPath = pathP.getDirPath(repositoryType, depId)
        #
        success = os.path.exists(dirPath)
        logger.info("success %r dirPath %r", success, dirPath)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(
            status_code=400, detail="File checking fails with %s" % str(e)
        )
    #
    if not success:
        if dirPath:
            raise HTTPException(
                status_code=404,
                detail="Request directory path does not exist %s" % dirPath,
            )
        else:
            raise HTTPException(
                status_code=400, detail="Bad or incomplete path metadata"
            )
    else:
        ret = {
            "success": success,
            "path": dirPath,
            "statusCode": 200,
            "statusMessage": "Directory exists",
        }

    return ret


@router.get("/path-exists", response_model=PathResult)
async def pathExists(
    path: str = Query(
        title="File or directory path",
        description="Full file or directory path",
        example="non_standard/directory/D_2000000001/D_2000000001_model_P1.cif.V1",
    ),
):
    """Check if a file exists provided a custom path, as opposed to standard input paramaters."""
    success = False
    try:
        # fU = FileUtil()
        logger.info("Checking if path exists %r", path)
        success = os.path.exists(path)
        # success = fU.exists(path)
        logger.info("success %r path %r", success, path)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        ret = {
            "path": path,
            "success": False,
            "statusCode": 400,
            "statusMessage": "File checking fails with %s" % str(e),
        }
    #
    if not success:
        if path:
            raise HTTPException(
                status_code=404, detail="Request path does not exist %s" % path
            )
        else:
            raise HTTPException(status_code=400, detail="No path provided in request")
    else:
        ret = {
            "path": path,
            "success": True,
            "statusCode": 200,
            "statusMessage": "Path exists",
        }

    return ret


@router.get("/latest-file-version", response_model=FileResult)
async def latestFileVersion(
    depId: str = Query(
        title="ID Code", description="Identifier code", example="D_0000000001"
    ),
    repositoryType: str = Query(
        title="Repository Type",
        description="OneDep repository type",
        example="onedep-archive, onedep-deposit",
    ),
    contentType: str = Query(
        title="Content type",
        description="OneDep content type",
        example="model, structure-factors, val-report-full",
    ),
    contentFormat: str = Query(
        title="Content format",
        description="OneDep content format",
        example="pdb, pdbx, mtz, pdf",
    ),
    partNumber: int = Query(
        1, title="Content part", description="OneDep part number", example="1,2,3"
    ),
    milestone: str = Query(
        "", title="milestone", description="milestone", example="release"
    ),
):
    success = False
    fileName = None
    filePath = None
    version = "latest"
    fileVersion = None
    try:
        # fU = FileUtil()
        # cP = ConfigProvider()
        pathP = PathProvider()
        #
        logger.info(
            "Getting latest file version for repositoryType %r depId %r contentType %r milestone %r partNumber %r format %r",
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
        )
        #
        filePath = pathP.getVersionedPath(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
        if not os.path.exists(filePath):
            logger.info(filePath)
            raise HTTPException(status_code=404, detail="Requested file does not exist")
        fileName = os.path.basename(filePath)
        # fileName = fU.getFileName(filePath)
        fileEnd = fileName.split(".")[-1]
        if "V" in fileEnd:
            fileVersion = fileEnd.split("V")[1]
        success = fileVersion is not None
        logger.info(
            "success %r fileName %r fileVersion %r", success, fileName, fileVersion
        )
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(
            status_code=400, detail="File checking fails with %s" % str(e)
        )
    #
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Unable to determine version for requested file (check query parameters)",
        )
    else:
        ret = {
            "success": True,
            "fileName": fileName,
            "version": fileVersion,
            "statusCode": 200,
            "statusMessage": "File exists",
        }

    return ret


@router.post("/copy-file")#, response_model=CopyFileResult)
async def copyFile(
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
    contentTypeSource: str = Form(
        title="Source Content type",
        description="OneDep content type of file to copy",
        example="model, structure-factors, val-report-full",
    ),
    milestoneSource: typing.Optional[str] = Form(),
    partNumberSource: int = Form(
        1,
        title="Source Content part",
        description="OneDep part number of file to copy",
        example="1,2,3",
    ),
    contentFormatSource: str = Form(
        title="Input Content format",
        description="OneDep content format of file to copy",
        example="pdb, pdbx, mtz, pdf",
    ),
    versionSource: str = Form(
        "latest",
        title="Source Version string",
        description="OneDep version number or description of file to copy",
        example="1,2,3, latest, previous, next",
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
    contentTypeTarget: str = Form(
        title="Target Content type",
        description="OneDep content type of destination file",
        example="model, structure-factors, val-report-full",
    ),
    milestoneTarget: typing.Optional[str] = Form(
        "", title="milestone", description="milestone", example="release"
    ),
    partNumberTarget: int = Form(
        1,
        title="Target Content part",
        description="OneDep part number of destination file",
        example="1,2,3",
    ),
    contentFormatTarget: str = Form(
        title="Input Content format",
        description="OneDep content format of destination file",
        example="pdb, pdbx, mtz, pdf",
    ),
    versionTarget: str = Form(
        None,
        title="Target Version string",
        description="OneDep version number or description of destination file",
        example="1,2,3, latest, previous, next",
    ),
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
    except Exception as exc:
        raise HTTPException(status_code=400, detail="error in copy file")
        return None
    return result

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
    logger.info("copy dir request %s %s %s %s", repositoryTypeSource, depIdSource, repositoryTypeTarget, depIdTarget)
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


@router.post("/move-file")  # response_model=CopyFileResult)
async def moveFile(
    repositoryTypeSource: str = Form(
        title="Source Repository Type",
        description="OneDep repository type of file to move",
        example="onedep-archive, onedep-deposit",
    ),
    depIdSource: str = Form(
        title="Source ID Code",
        description="Identifier code of file to move",
        example="D_0000000001",
    ),
    contentTypeSource: str = Form(
        title="Source Content type",
        description="OneDep content type of file to move",
        example="model, structure-factors, val-report-full",
    ),
    partNumberSource: int = Form(
        1,
        title="Source Content part",
        description="OneDep part number of file to move",
        example="1,2,3",
    ),
    milestoneSource: str = Form(""),
    contentFormatSource: str = Form(
        title="Input Content format",
        description="OneDep content format of file to move",
        example="pdb, pdbx, mtz, pdf",
    ),
    versionSource: str = Form(
        "latest",
        title="Source Version string",
        description="OneDep version number or description of file to move",
        example="1,2,3, latest, previous, next",
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
    contentTypeTarget: str = Form(
        title="Target Content type",
        description="OneDep content type of destination file",
        example="model, structure-factors, val-report-full",
    ),
    partNumberTarget: int = Form(
        1,
        title="Target Content part",
        description="OneDep part number of destination file",
        example="1,2,3",
    ),
    milestoneTarget: str = Form(
        "", title="milestone", description="milestone", example="release"
    ),
    contentFormatTarget: str = Form(
        title="Input Content format",
        description="OneDep content format of destination file",
        example="pdb, pdbx, mtz, pdf",
    ),
    versionTarget: str = Form(
        None,
        title="Target Version string",
        description="OneDep version number or description of destination file",
        example="1,2,3, latest, previous, next",
    ),
    #
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
        # cP = ConfigProvider()
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
                # fU.remove(dirPath)
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
                # fU.remove(dirPath)
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
