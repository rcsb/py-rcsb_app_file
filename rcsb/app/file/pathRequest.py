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


# Add Endpoints:
# - fileExists (use for checking if file exists provided ID, repoType, contentType, etc.--client focused)
# - pathExists (use for checking if file or directory exists provided an absolute path--general purpose)
# - getLatestFileVersion
# - Move files between directories
# - dirExists  (use for checking if directory exists provided the ID and repoType--client focused)
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
