##
# File: pathRequest.py
# Date: 24-May-2022
#
# Add Endpoints:
#   - fileExists (use for checking if file exists provided ID, repoType, contentType, etc.--client focused)
#   - pathExists (use for checking if file or directory exists provided an absolute path--general purpose)
#   - dirExists  (use for checking if directory exists provided the ID and repoType--client focused)
#   - getLatestFileVersion
#   - Copy and/or move files between directories
#   - List directory
#   - getFileHash
#
##
__docformat__ = "google en"
__author__ = "Dennis Piehl"
__email__ = "dennis.piehl@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
from enum import Enum

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Query
from fastapi import HTTPException
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.app.file.PathUtils import PathUtils
from rcsb.utils.io.FileUtil import FileUtil

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["status"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class FileResult(BaseModel):
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    fileName: str = Field(None, title="Stored file name", description="Stored file name", example="D_0000000001_model_P1.cif.V3")
    version: str = Field(None, title="Stored file version", description="Stored file version", example="1,2,3")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


class DirResult(BaseModel):
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    dirPath: str = Field(None, title="Directory path", description="Directory path to list", example="repository/archive/D_2000000001/")
    dirList: list = Field(None, title="Directory list", description="Directory content list", example=["D_0000000001_model_P1.cif.V1", "D_0000000001_model_P1.cif.V2"])
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


class PathResult(BaseModel):
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    path: str = Field(None, title="Path", description="File or directory path", example="repository/archive/D_2000000001/D_2000000001_model_P1.cif.V1")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


class FileCopyResult(BaseModel):
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    filePathSource: str = Field(None, title="Source file path", description="Stored file name", example="D_0000000001_model_P1.cif.V3")
    filePathTarget: str = Field(None, title="Target file path", description="Stored file name", example="D_0000000001_model_P1.cif.V3")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


@router.post("/file-exists", response_model=FileResult)
async def fileExists(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(None, title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    contentType: str = Query(None, title="Content type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Query(None, title="Content format", description="OneDep content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Query(1, title="Content part", description="OneDep part number", example="1,2,3"),
    fileName: str = Query(None, title="Filename", description="Filename", example="example.cif.gz"),
    dirPath: str = Query(None, title="File directory", description="File directory", example="/non_standard/directory/"),
    filePath: str = Query(None, title="File path", description="Full file path", example="/non_standard/directory/example.cif.gz"),
    version: str = Query("latest", title="Version string", description="OneDep version number or description", example="1,2,3, latest, previous"),
):
    success = False
    try:
        fU = FileUtil()
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        #
        if not filePath:
            if dirPath and fileName:
                logger.info("Checking dirPath %r fileName %r", dirPath, fileName)
                filePath = os.path.join(dirPath, fileName)
            else:
                logger.info("Checking repositoryType %r idCode %r contentType %r format %r version %r", repositoryType, idCode, contentType, contentFormat, version)
                filePath = pathU.getVersionedPath(repositoryType, idCode, contentType, partNumber, contentFormat, version)
        else:
            logger.info("Checking filePath %r", filePath)
        fileName = fU.getFileName(filePath)
        # fileEnd = fileName.split(".")[-1]
        # if "V" in fileEnd:
        #     fileVersion = fileEnd.split("V")[1]
        success = fU.exists(filePath)
        logger.info("success %r fileName %r filepath %r", success, fileName, filePath)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        if filePath:
            raise HTTPException(status_code=404, detail="Request file path does not exist %s" % filePath)
        else:
            raise HTTPException(status_code=403, detail="Bad or incomplete path metadata")
    else:
        ret = {"success": success, "fileName": fileName, "version": version, "statusCode": 200, "statusMessage": "File exists"}

    return ret


@router.post("/dir-exists", response_model=PathResult)
async def dirExists(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(None, title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    dirPath: str = Query(None, title="File directory", description="File directory path", example="/non_standard/directory/"),
):
    success = False
    try:
        fU = FileUtil()
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        #
        if not dirPath:
            logger.info("Checking repositoryType %r idCode %r", repositoryType, idCode)
            dirPath = pathU.getDirPath(repositoryType, idCode)
        else:
            logger.info("Checking dirPath %r", dirPath)
        #
        success = fU.exists(dirPath)
        logger.info("success %r dirPath %r", success, dirPath)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        if dirPath:
            raise HTTPException(status_code=404, detail="Request directory path does not exist %s" % dirPath)
        else:
            raise HTTPException(status_code=403, detail="Bad or incomplete path metadata")
    else:
        ret = {"success": success, "path": dirPath, "statusCode": 200, "statusMessage": "Directory exists"}

    return ret


@router.post("/path-exists", response_model=PathResult)
async def pathExists(
    path: str = Query(None, title="File path", description="Full file or directory path", example="non_standard/directory/D_2000000001/D_2000000001_model_P1.cif.V1"),

):
    success = False
    try:
        fU = FileUtil()
        logger.info("Checking if path exists %r", path)
        success = fU.exists(path)
        logger.info("success %r path %r", success, path)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        ret = {"path": path, "success": False, "statusCode": 400, "statusMessage": "File checking fails with %s" % str(e)}
    #
    if not success:
        if path:
            raise HTTPException(status_code=404, detail="Request path does not exist %s" % path)
        else:
            raise HTTPException(status_code=403, detail="No path provided in request")
    else:
        ret = {"path": path, "success": True, "statusCode": 200, "statusMessage": "Path exists"}

    return ret


@router.get("/latest-file-version", response_model=FileResult)
async def latestFileVersion(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(None, title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    contentType: str = Query(None, title="Content type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Query(None, title="Content format", description="OneDep content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Query(1, title="Content part", description="OneDep part number", example="1,2,3"),
):
    success = False
    fileName = None
    filePath = None
    version = "latest"
    fileVersion = None
    try:
        fU = FileUtil()
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        #
        logger.info(
            "Getting latest file version for repositoryType %r idCode %r contentType %r format %r",
            repositoryType,
            idCode,
            contentType,
            contentFormat,
        )
        #
        filePath = pathU.getVersionedPath(repositoryType, idCode, contentType, partNumber, contentFormat, version)
        if not fU.exists(filePath):
            raise HTTPException(status_code=404, detail="Requested file does not exist")
        fileName = fU.getFileName(filePath)
        fileEnd = fileName.split(".")[-1]
        if "V" in fileEnd:
            fileVersion = fileEnd.split("V")[1]
        success = fileVersion is not None
        logger.info("success %r fileName %r fileVersion %r", success, fileName, fileVersion)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        raise HTTPException(status_code=400, detail="Unable to determine version for requested file (check query parameters)")
    else:
        ret = {"success": True, "fileName": fileName, "version": fileVersion, "statusCode": 200, "statusMessage": "File exists"}

    return ret


@router.post("/file-copy", response_model=FileCopyResult)
async def fileCopy(
    idCodeSource: str = Query(None, title="Source ID Code", description="Identifier code of file to copy", example="D_0000000001"),
    repositoryTypeSource: str = Query(None, title="Source Repository Type", description="OneDep repository type of file to copy", example="onedep-archive, onedep-deposit"),
    contentTypeSource: str = Query(None, title="Source Content type", description="OneDep content type of file to copy", example="model, structure-factors, val-report-full"),
    contentFormatSource: str = Query(None, title="Input Content format", description="OneDep content format of file to copy", example="pdb, pdbx, mtz, pdf"),
    partNumberSource: int = Query(1, title="Source Content part", description="OneDep part number of file to copy", example="1,2,3"),
    fileNameSource: str = Query(None, title="Source Filename", description="Filename of file to copy", example="example.cif.gz"),
    dirPathSource: str = Query(None, title="Source File directory", description="File directory of file to copy", example="/non_standard/directory/"),
    filePathSource: str = Query(None, title="Source File path", description="Full file path of file to copy", example="/non_standard/directory/example.cif.gz"),
    versionSource: str = Query("latest", title="Source Version string", description="OneDep version number or description of file to copy", example="1,2,3, latest, previous, next"),
    #
    idCodeTarget: str = Query(None, title="Target ID Code", description="Identifier code of destination file", example="D_0000000001"),
    repositoryTypeTarget: str = Query(None, title="Target Repository Type", description="OneDep repository type of destination file", example="onedep-archive, onedep-deposit"),
    contentTypeTarget: str = Query(None, title="Target Content type", description="OneDep content type of destination file", example="model, structure-factors, val-report-full"),
    contentFormatTarget: str = Query(None, title="Input Content format", description="OneDep content format of destination file", example="pdb, pdbx, mtz, pdf"),
    partNumberTarget: int = Query(1, title="Target Content part", description="OneDep part number of destination file", example="1,2,3"),
    fileNameTarget: str = Query(None, title="Target Filename", description="Filename of destination file", example="example.cif.gz"),
    dirPathTarget: str = Query(None, title="Target File directory", description="File directory of destination file", example="/non_standard/directory/"),
    filePathTarget: str = Query(None, title="Target File path", description="Full file path of destination file", example="/non_standard/directory/example.cif.gz"),
    versionTarget: str = Query(None, title="Target Version string", description="OneDep version number or description of destination file", example="1,2,3, latest, previous, next"),
):
    success = False
    try:
        fU = FileUtil()
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        #
        if not filePathSource:
            if dirPathSource and fileNameSource:
                logger.info("Copying dirPath %r fileName %r", dirPathSource, fileNameSource)
                filePathSource = os.path.join(dirPathSource, fileNameSource)
            else:
                logger.info(
                    "Copying repositoryType %r idCode %r contentType %r format %r version %r",
                    repositoryTypeSource, idCodeSource, contentTypeSource, contentFormatSource, versionSource
                )
                filePathSource = pathU.getVersionedPath(repositoryTypeSource, idCodeSource, contentTypeSource, partNumberSource, contentFormatSource, versionSource)
                logger.info("filePathSource %r", filePathSource)
        if not filePathTarget:
            if dirPathTarget and fileNameTarget:
                logger.info("Destination dirPath %r fileName %r", dirPathTarget, fileNameTarget)
                filePathTarget = os.path.join(dirPathTarget, fileNameTarget)
            else:
                if not versionTarget:
                    sourceFileEnd = filePathSource.split(".")[-1]
                    if "V" in sourceFileEnd:
                        # set target version to the same as source version
                        versionTarget = sourceFileEnd.split("V")[1]
                    else:
                        # set target version to "next" increment in target repo (if file doesn't already exist in the target repo, then it will start at "V1")
                        versionTarget = "next"
                logger.info(
                    "Destination repositoryType %r idCode %r contentType %r format %r version %r",
                    repositoryTypeTarget, idCodeTarget, contentTypeTarget, contentFormatTarget, versionTarget
                )
                filePathTarget = pathU.getVersionedPath(repositoryTypeTarget, idCodeTarget, contentTypeTarget, partNumberTarget, contentFormatTarget, versionTarget)
                logger.info("filePathTarget %r", filePathTarget)

        if not filePathSource or not filePathTarget:
            raise HTTPException(status_code=403, detail="Source (%r) or target (%r) filepath not defined" % (filePathSource, filePathTarget))

        logger.info("Copying filePath %r to %r", filePathSource, filePathTarget)
        success = fU.put(filePathSource, filePathTarget)

        logger.info("success %r filePathSource %r filePathTarget %r", success, filePathSource, filePathTarget)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        raise HTTPException(status_code=403, detail="Bad or incomplete request parameters")
    else:
        ret = {"success": success, "filePathSource": filePathSource, "filePathTarget": filePathTarget, "statusCode": 200, "statusMessage": "File copy success"}

    return ret


@router.post("/list-dir", response_model=DirResult)
async def listDir(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(None, title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    dirPath: str = Query(None, title="File directory", description="File directory", example="/non_standard/directory/"),
    filePath: str = Query(None, title="File path", description="Full file path", example="/non_standard/directory/example.cif.gz"),
):
    success = False
    dirList = []
    try:
        fU = FileUtil()
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        #
        if not dirPath:
            if filePath:
                # List the parent directory of the requested filePath
                dirPath = os.path.abspath(os.path.dirname(filePath))
                logger.info("Listing dirPath %r for filePath %r", dirPath, filePath)
            else:
                # List directory of requested repositoryType and idCode
                dirPath = pathU.getDirPath(repositoryType, idCode)
                logger.info("Listing dirPath %r for repositoryType %r idCode %r", dirPath, repositoryType, idCode)
        else:
            logger.info("Listing dirPath %r", dirPath)
        dirExistsBool = fU.exists(dirPath)
        if dirExistsBool:
            dirList = os.listdir(dirPath)
            logger.info("dirList (len %d): %r", len(dirList), dirList)
            success = True
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        if not dirExistsBool:
            raise HTTPException(status_code=404, detail="Requested directory does not exist %s" % dirPath)
        else:
            raise HTTPException(status_code=403, detail="Failed to list directory for given request parameters")
    else:
        ret = {"success": success, "dirPath": dirPath, "dirList": dirList, "statusCode": 200, "statusMessage": "Directory contents"}

    return ret
