##
# File: pathRequest.py
# Date: 24-May-2022
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


# Add Endpoints:
# - fileExists (use for checking if file exists provided ID, repoType, contentType, etc.--client focused)
# - pathExists (use for checking if file or directory exists provided an absolute path--general purpose)
# - dirExists  (use for checking if directory exists provided the ID and repoType--client focused)
# - getLatestFileVersion
# - Move and/or copy files between directories
# - getFileHash


@router.post("/file-exists", response_model=FileResult)
async def fileExists(
    idCode: str = Query(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(None, title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    contentType: str = Query(None, title="Content type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Query(None, title="Content format", description="OneDep content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Query(1, title="Content part", description="OneDep part number", example="1,2,3"),
    fileName: str = Query(None, title="Filename", description="Filename", example="example.cif.gz"),
    fileDir: str = Query(None, title="File directory", description="File directory", example="/non_standard/directory/"),
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
            if fileDir and fileName:
                logger.info("Checking fileDir %r fileName %r", fileDir, fileName)
                filePath = os.path.join(fileDir, fileName)
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
    fileDir: str = Query(None, title="File directory", description="File directory path", example="/non_standard/directory/"),
):
    success = False
    try:
        fU = FileUtil()
        cachePath = os.environ.get("CACHE_PATH")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        pathU = PathUtils(cP)
        #
        if not fileDir:
            logger.info("Checking repositoryType %r idCode %r", repositoryType, idCode)
            fileDir = pathU.getDirPath(repositoryType, idCode)
        else:
            logger.info("Checking fileDir %r", fileDir)
        #
        success = fU.exists(fileDir)
        logger.info("success %r fileDir %r", success, fileDir)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        if fileDir:
            raise HTTPException(status_code=404, detail="Request directory path does not exist %s" % fileDir)
        else:
            raise HTTPException(status_code=403, detail="Bad or incomplete path metadata")
    else:
        ret = {"success": success, "path": fileDir, "statusCode": 200, "statusMessage": "Directory exists"}

    return ret


@router.post("/path-exists", response_model=PathResult)
async def pathExists(
    filePath: str = Query(None, title="File path", description="Full file or directory path", example="non_standard/directory/D_2000000001/D_2000000001_model_P1.cif.V1"),

):
    success = False
    try:
        fU = FileUtil()
        logger.info("Checking if path exists %r", filePath)
        success = fU.exists(filePath)
        logger.info("success %r path %r", success, filePath)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        ret = {"path": filePath, "success": False, "statusCode": 400, "statusMessage": "File checking fails with %s" % str(e)}
    #
    if not success:
        if filePath:
            raise HTTPException(status_code=404, detail="Request path does not exist %s" % filePath)
        else:
            raise HTTPException(status_code=403, detail="No path provided in request")
    else:
        ret = {"path": filePath, "success": True, "statusCode": 200, "statusMessage": "Path exists"}

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
    idCodeSource: str = Query(None, title="Input ID Code", description="Identifier code of file to copy", example="D_0000000001"),
    repositoryTypeSource: str = Query(None, title="Input Repository Type", description="OneDep repository type of file to copy", example="onedep-archive, onedep-deposit"),
    contentTypeSource: str = Query(None, title="Input Content type", description="OneDep content type of file to copy", example="model, structure-factors, val-report-full"),
    contentFormatSource: str = Query(None, title="Input Content format", description="OneDep content format of file to copy", example="pdb, pdbx, mtz, pdf"),
    partNumberSource: int = Query(1, title="Input Content part", description="OneDep part number of file to copy", example="1,2,3"),
    fileNameSource: str = Query(None, title="Input Filename", description="Filename of file to copy", example="example.cif.gz"),
    fileDirSource: str = Query(None, title="Input File directory", description="File directory of file to copy", example="/non_standard/directory/"),
    filePathSource: str = Query(None, title="Input File path", description="Full file path of file to copy", example="/non_standard/directory/example.cif.gz"),
    versionSource: str = Query("latest", title="Input Version string", description="OneDep version number or description of file to copy", example="1,2,3, latest, previous"),
    #
    idCodeTarget: str = Query(None, title="Input ID Code", description="Identifier code of destination file", example="D_0000000001"),
    repositoryTypeTarget: str = Query(None, title="Input Repository Type", description="OneDep repository type of destination file", example="onedep-archive, onedep-deposit"),
    contentTypeTarget: str = Query(None, title="Input Content type", description="OneDep content type of destination file", example="model, structure-factors, val-report-full"),
    contentFormatTarget: str = Query(None, title="Input Content format", description="OneDep content format of destination file", example="pdb, pdbx, mtz, pdf"),
    partNumberTarget: int = Query(1, title="Input Content part", description="OneDep part number of destination file", example="1,2,3"),
    fileNameTarget: str = Query(None, title="Input Filename", description="Filename of destination file", example="example.cif.gz"),
    fileDirTarget: str = Query(None, title="Input File directory", description="File directory of destination file", example="/non_standard/directory/"),
    filePathTarget: str = Query(None, title="Input File path", description="Full file path of destination file", example="/non_standard/directory/example.cif.gz"),
    versionTarget: str = Query("latest", title="Input Version string", description="OneDep version number or description of destination file", example="1,2,3, latest, previous"),
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
            if fileDirSource and fileNameSource:
                logger.info("Copying fileDir %r fileName %r", fileDirSource, fileNameSource)
                filePathSource = os.path.join(fileDirSource, fileNameSource)
            else:
                logger.info(
                    "Copying repositoryType %r idCode %r contentType %r format %r version %r",
                    repositoryTypeSource, idCodeSource, contentTypeSource, contentFormatSource, versionSource
                )
                filePathSource = pathU.getVersionedPath(repositoryTypeSource, idCodeSource, contentTypeSource, partNumberSource, contentFormatSource, versionSource)
        if not filePathTarget:
            if fileDirTarget and fileNameTarget:
                logger.info("Destination fileDir %r fileName %r", fileDirTarget, fileNameTarget)
                filePathTarget = os.path.join(fileDirTarget, fileNameTarget)
            else:
                logger.info(
                    "Destination repositoryType %r idCode %r contentType %r format %r version %r",
                    repositoryTypeTarget, idCodeTarget, contentTypeTarget, contentFormatTarget, versionTarget
                )
                filePathTarget = pathU.getVersionedPath(repositoryTypeTarget, idCodeTarget, contentTypeTarget, partNumberTarget, contentFormatTarget, versionTarget)

        if not (filePathSource or filePathTarget):
            raise HTTPException(status_code=403, detail="Source (%r) or target (%r) filepath not defined." % (filePathSource, filePathTarget))

        logger.info("Copying filePath %r to %r", filePathSource, filePathTarget)
        success = fU.put(filePathSource, filePathTarget)

        logger.info("success %r filePathSource %r filePathTarget %r", success, filePathSource, filePathTarget)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        raise HTTPException(status_code=403, detail="Bad or incomplete payload")
    else:
        ret = {"success": success, "filePathSource": filePathSource, "filePathTarget": filePathTarget, "statusCode": 200, "statusMessage": "File copy success"}

    return ret
