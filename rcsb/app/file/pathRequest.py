##
# File: pathRequest.py
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
from enum import Enum

from fastapi import APIRouter, Form
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


class CompressResult(BaseModel):
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    dirPath: str = Field(None, title="Directory path", description="Directory path to list", example="repository/archive/D_2000000001/")
    compressPath: str = Field(None, title="Compressed directory path", description="Compressed directory path", example="repository/archive/D_0000000001.tar.gz")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


class PathResult(BaseModel):
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    path: str = Field(None, title="Path", description="File or directory path", example="repository/archive/D_2000000001/D_2000000001_model_P1.cif.V1")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


class CopyFileResult(BaseModel):
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    filePathSource: str = Field(None, title="Source file path", description="Stored file name", example="D_0000000001_model_P1.cif.V3")
    filePathTarget: str = Field(None, title="Target file path", description="Stored file name", example="D_0000000001_model_P1.cif.V3")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


@router.post("/file-exists", response_model=FileResult)
async def fileExists(
    depId: str = Query(title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    contentType: str = Query(title="Content type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Query(title="Content format", description="OneDep content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Query(1, title="Content part", description="OneDep part number", example="1,2,3"),
    version: str = Query("latest", title="Version string", description="OneDep version number or description", example="1,2,3, latest, previous"),
    milestone: str = Query("", title="milestone", description="milestone", example="release"),
):
    """Check if a file exists provided standard file parameters.
    """
    success = False
    try:
        fU = FileUtil()
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
        #
        logger.info("Checking repositoryType %r depId %r contentType %r milestone %r format %r version %r", repositoryType, depId, contentType, milestone, contentFormat, version)
        filePath = pathU.getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)
        logger.info("Checking filePath %r", filePath)
        fileName = fU.getFileName(filePath)
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
            raise HTTPException(status_code=400, detail="Bad or incomplete path metadata")
    else:
        ret = {"success": success, "fileName": fileName, "version": version, "statusCode": 200, "statusMessage": "File exists"}

    return ret


@router.post("/dir-exists", response_model=PathResult)
async def dirExists(
    depId: str = Query(title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
):
    """Check if a file exists provided standard directory parameters.
    """
    success = False
    try:
        fU = FileUtil()
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
        #
        logger.info("Checking repositoryType %r depId %r", repositoryType, depId)
        dirPath = pathU.getDirPath(repositoryType, depId)
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
            raise HTTPException(status_code=400, detail="Bad or incomplete path metadata")
    else:
        ret = {"success": success, "path": dirPath, "statusCode": 200, "statusMessage": "Directory exists"}

    return ret


@router.post("/path-exists", response_model=PathResult)
async def pathExists(
    path: str = Query(title="File or directory path", description="Full file or directory path", example="non_standard/directory/D_2000000001/D_2000000001_model_P1.cif.V1"),

):
    """Check if a file exists provided a custom path, as opposed to standard input paramaters.
    """
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
            raise HTTPException(status_code=400, detail="No path provided in request")
    else:
        ret = {"path": path, "success": True, "statusCode": 200, "statusMessage": "Path exists"}

    return ret


@router.get("/latest-file-version", response_model=FileResult)
async def latestFileVersion(
    depId: str = Query(title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    contentType: str = Query(title="Content type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    contentFormat: str = Query(title="Content format", description="OneDep content format", example="pdb, pdbx, mtz, pdf"),
    partNumber: int = Query(1, title="Content part", description="OneDep part number", example="1,2,3"),
    milestone: str = Query("", title="milestone", description="milestone", example="release"),
):
    success = False
    fileName = None
    filePath = None
    version = "latest"
    fileVersion = None
    try:
        fU = FileUtil()
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
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
        filePath = pathU.getVersionedPath(repositoryType, depId, contentType, milestone, partNumber, contentFormat, version)
        if not fU.exists(filePath):
            logger.info(filePath)
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


@router.post("/copy-file", response_model=CopyFileResult)
async def copyFile(
    depIdSource: str = Query(title="Source ID Code", description="Identifier code of file to copy", example="D_0000000001"),
    repositoryTypeSource: str = Query(title="Source Repository Type", description="OneDep repository type of file to copy", example="onedep-archive, onedep-deposit"),
    contentTypeSource: str = Query(title="Source Content type", description="OneDep content type of file to copy", example="model, structure-factors, val-report-full"),
    contentFormatSource: str = Query(title="Input Content format", description="OneDep content format of file to copy", example="pdb, pdbx, mtz, pdf"),
    partNumberSource: int = Query(1, title="Source Content part", description="OneDep part number of file to copy", example="1,2,3"),
    versionSource: str = Query("latest", title="Source Version string", description="OneDep version number or description of file to copy", example="1,2,3, latest, previous, next"),
    #
    depIdTarget: str = Query(title="Target ID Code", description="Identifier code of destination file", example="D_0000000001"),
    repositoryTypeTarget: str = Query(title="Target Repository Type", description="OneDep repository type of destination file", example="onedep-archive, onedep-deposit"),
    contentTypeTarget: str = Query(title="Target Content type", description="OneDep content type of destination file", example="model, structure-factors, val-report-full"),
    contentFormatTarget: str = Query(title="Input Content format", description="OneDep content format of destination file", example="pdb, pdbx, mtz, pdf"),
    partNumberTarget: int = Query(1, title="Target Content part", description="OneDep part number of destination file", example="1,2,3"),
    versionTarget: str = Query(None, title="Target Version string", description="OneDep version number or description of destination file", example="1,2,3, latest, previous, next"),
    milestone: str = Query("", title="milestone", description="milestone", example="release"),
):
    """Copy a file given standard input paramaters for both the source and destination of the file.
    """
    success = False
    try:
        fU = FileUtil()
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
        #
        logger.info(
            "Copying repositoryType %r depId %r contentType %r milestone %r format %r version %r",
            repositoryTypeSource, depIdSource, contentTypeSource, milestone, contentFormatSource, versionSource
        )
        filePathSource = pathU.getVersionedPath(repositoryTypeSource, depIdSource, contentTypeSource, milestone, partNumberSource, contentFormatSource, versionSource)
        logger.info("filePathSource %r", filePathSource)
        if not versionTarget:
            sourceFileEnd = filePathSource.split(".")[-1]
            if "V" in sourceFileEnd:
                # set target version to the same as source version
                versionTarget = sourceFileEnd.split("V")[1]
        logger.info(
            "Destination repositoryType %r depId %r contentType %r milestone %r format %r version %r",
            repositoryTypeTarget, depIdTarget, contentTypeTarget, milestone, contentFormatTarget, versionTarget
        )
        filePathTarget = pathU.getVersionedPath(repositoryTypeTarget, depIdTarget, contentTypeTarget, milestone, partNumberTarget, contentFormatTarget, versionTarget)
        logger.info("filePathTarget %r", filePathTarget)

        if not filePathSource or not filePathTarget:
            raise ValueError("Source (%r) or target (%r) filepath not defined" % (filePathSource, filePathTarget))

        logger.info("Copying filePath %r to %r", filePathSource, filePathTarget)
        success = fU.put(filePathSource, filePathTarget)

        logger.info("success %r filePathSource %r filePathTarget %r", success, filePathSource, filePathTarget)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        raise HTTPException(status_code=400, detail="Bad or incomplete request parameters")
    else:
        ret = {"success": success, "filePathSource": filePathSource, "filePathTarget": filePathTarget, "statusCode": 200, "statusMessage": "File copy success"}

    return ret


@router.post("/copy-filepath", response_model=CopyFileResult)
async def copyFilePath(
    filePathSource: str = Query(title="Source File path", description="Full file path of file to copy", example="/non_standard/directory/example.cif.gz"),
    filePathTarget: str = Query(title="Target File path", description="Full file path of destination file", example="/non_standard/directory/example.cif.gz"),
):
    """Copy a file given its explicit source and destination path (as opposed to using standard input paramaters).
    """
    success = False
    try:
        fU = FileUtil()
        logger.info("Copying filePath %r to %r", filePathSource, filePathTarget)
        success = fU.put(filePathSource, filePathTarget)
        logger.info("success %r filePathSource %r filePathTarget %r", success, filePathSource, filePathTarget)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        raise HTTPException(status_code=400, detail="Bad or incomplete request parameters")
    else:
        ret = {"success": success, "filePathSource": filePathSource, "filePathTarget": filePathTarget, "statusCode": 200, "statusMessage": "File copy success"}

    return ret


@router.post("/move-file")  # response_model=CopyFileResult)
async def moveFile(
    depIdSource: str = Form(title="Source ID Code", description="Identifier code of file to move", example="D_0000000001"),
    repositoryTypeSource: str = Form(title="Source Repository Type", description="OneDep repository type of file to move", example="onedep-archive, onedep-deposit"),
    contentTypeSource: str = Form(title="Source Content type", description="OneDep content type of file to move", example="model, structure-factors, val-report-full"),
    contentFormatSource: str = Form(title="Input Content format", description="OneDep content format of file to move", example="pdb, pdbx, mtz, pdf"),
    partNumberSource: int = Form(1, title="Source Content part", description="OneDep part number of file to move", example="1,2,3"),
    versionSource: str = Form("latest", title="Source Version string", description="OneDep version number or description of file to move", example="1,2,3, latest, previous, next"),
    milestoneSource: str = Form(""),
    depIdTarget: str = Form(title="Target ID Code", description="Identifier code of destination file", example="D_0000000001"),
    repositoryTypeTarget: str = Form(title="Target Repository Type", description="OneDep repository type of destination file", example="onedep-archive, onedep-deposit"),
    contentTypeTarget: str = Form(title="Target Content type", description="OneDep content type of destination file", example="model, structure-factors, val-report-full"),
    contentFormatTarget: str = Form(title="Input Content format", description="OneDep content format of destination file", example="pdb, pdbx, mtz, pdf"),
    partNumberTarget: int = Form(1, title="Target Content part", description="OneDep part number of destination file", example="1,2,3"),
    versionTarget: str = Form(None, title="Target Version string", description="OneDep version number or description of destination file", example="1,2,3, latest, previous, next"),
    milestoneTarget: str = Form("", title="milestone", description="milestone", example="release"),
):
    """Move a file given standard input paramaters for both the source and destination of the file.
    """
    success = False
    try:
        fU = FileUtil()
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
        #
        logger.info(
            "Moving repositoryType %r depId %r contentType %r milestone %r format %r version %r",
            repositoryTypeSource, depIdSource, contentTypeSource, milestoneSource, contentFormatSource, versionSource
        )
        filePathSource = pathU.getVersionedPath(repositoryTypeSource, depIdSource, contentTypeSource, milestoneSource, partNumberSource, contentFormatSource, versionSource)
        logger.info("filePathSource %r", filePathSource)
        if not versionTarget:
            sourceFileEnd = filePathSource.split(".")[-1]
            if "V" in sourceFileEnd:
                # set target version to the same as source version
                versionTarget = sourceFileEnd.split("V")[1]
        logger.info(
            "Destination repositoryType %r depId %r contentType %r milestone %r format %r version %r",
            repositoryTypeTarget, depIdTarget, contentTypeTarget, milestoneTarget, contentFormatTarget, versionTarget
        )
        filePathTarget = pathU.getVersionedPath(repositoryTypeTarget, depIdTarget, contentTypeTarget, milestoneTarget, partNumberTarget, contentFormatTarget, versionTarget)
        logger.info("filePathTarget %r", filePathTarget)

        if not filePathSource or not filePathTarget:
            raise ValueError("Source (%r) or target (%r) filepath not defined" % (filePathSource, filePathTarget))

        logger.info("Moving filePath %r to %r", filePathSource, filePathTarget)
        ok1 = fU.mkdirForFile(filePathTarget)
        ok2 = fU.replace(filePathSource, filePathTarget)
        success = ok1 and ok2

        logger.info("success %r (make dest dir %r, move file %r) filePathSource %r filePathTarget %r", success, ok1, ok2, filePathSource, filePathTarget)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))

    if not success:
        raise HTTPException(status_code=400, detail="Bad or incomplete request parameters")
    else:
        ret = {"success": success, "filePathSource": filePathSource, "filePathTarget": filePathTarget, "statusCode": 200, "statusMessage": "File move success"}

    return ret


@router.post("/move-filepath", response_model=CopyFileResult)
async def moveFilePath(
    filePathSource: str = Query(title="Source File path", description="Full file path of file to move", example="/non_standard/directory/example.cif.gz"),
    filePathTarget: str = Query(title="Target File path", description="Full file path of destination file", example="/non_standard/directory/example.cif.gz"),
):
    """Move a file given its explicit source and destination path (as opposed to using standard input paramaters).
    """
    success = False
    try:
        fU = FileUtil()
        logger.info("Moving filePath %r to %r", filePathSource, filePathTarget)
        ok1 = fU.mkdirForFile(filePathTarget)
        ok2 = fU.replace(filePathSource, filePathTarget)
        success = ok1 and ok2

        logger.info("success %r (make dest dir %r, move file %r) filePathSource %r filePathTarget %r", success, ok1, ok2, filePathSource, filePathTarget)
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="File checking fails with %s" % str(e))
    #
    if not success:
        raise HTTPException(status_code=400, detail="Bad or incomplete request parameters")
    else:
        ret = {"success": success, "filePathSource": filePathSource, "filePathTarget": filePathTarget, "statusCode": 200, "statusMessage": "File move success"}

    return ret


@router.get("/list-dir", response_model=DirResult)
async def listDir(
    repositoryType: str = Query(title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
    depId: str = Query(title="ID Code", description="Identifier code", example="D_0000000001")
):
    """List files in directory of requested depId and repositoryType (using standard input paramaters).
    """
    success = False
    dirList = []
    dirExistsCheck = None
    try:
        fU = FileUtil()
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
        #
        # List directory of requested repositoryType and depId
        dirPath = pathU.getDirPath(repositoryType, depId)
        logger.info("Listing dirPath %r for repositoryType %r depId %r", dirPath, repositoryType, depId)
        dirExistsCheck = fU.exists(dirPath)
        if dirExistsCheck:
            dirList = os.listdir(dirPath)
            success = True
    #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="Directory listing fails with %s" % str(e))
    #
    if not success:
        if dirExistsCheck is False:
            logger.info("dirExistsCheck is %r", dirExistsCheck)
            raise HTTPException(status_code=404, detail="Requested directory does not exist %s" % dirPath)
        else:
            raise HTTPException(status_code=400, detail="Failed to list directory for given request parameters")
    else:
        ret = {"success": success, "dirPath": dirPath, "dirList": dirList, "statusCode": 200, "statusMessage": "Directory contents"}

    return ret


@router.get("/list-dirpath", response_model=DirResult)
async def listDirPath(
    dirPath: str = Query(title="Directory path", description="Full directory path", example="non_standard/directory/D_2000000001/"),
):
    """List files in requested explicit path, as opposed to standard input paramaters.
    """
    success = False
    dirList = []
    dirExistsCheck = None
    try:
        fU = FileUtil()
        logger.info("Listing dirPath %r", dirPath)
        dirExistsCheck = fU.exists(dirPath)
        if dirExistsCheck:
            dirList = os.listdir(dirPath)
            logger.info("dirList (len %d): %r", len(dirList), dirList)
            success = True
    #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="Directory listing fails with %s" % str(e))
    #
    if not success:
        if dirExistsCheck is False:
            logger.info("dirExistsCheck is %r", dirExistsCheck)
            raise HTTPException(status_code=404, detail="Requested directory does not exist %s" % dirPath)
        else:
            raise HTTPException(status_code=400, detail="Failed to list directory for given request parameters")
    else:
        ret = {"success": success, "dirPath": dirPath, "dirList": dirList, "statusCode": 200, "statusMessage": "Directory contents"}

    return ret


@router.post("/compress-dir", response_model=CompressResult)
async def compressDir(
    depId: str = Query(title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Query(title="Repository Type", description="OneDep repository type", example="onedep-archive, onedep-deposit"),
):
    """Compress directory of requested depId and repositoryType (using standard input paramaters).
    """
    success = False
    compressPath = None
    dirExistsCheck = None
    dirRemovedBool = None
    try:
        fU = FileUtil()
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        pathU = PathUtils(cP)
        #
        # Compress directory of requested repositoryType and depId
        dirPath = pathU.getDirPath(repositoryType, depId)
        logger.info("Compressing dirPath %r for repositoryType %r depId %r", dirPath, repositoryType, depId)
        dirExistsCheck = fU.exists(dirPath)
        if dirExistsCheck:
            compressPath = os.path.abspath(dirPath) + ".tar.gz"
            ok = fU.bundleTarfile(compressPath, [os.path.abspath(dirPath)])
            if ok:
                logger.info("created compressPath %s from dirPath %s", compressPath, dirPath)
                fU.remove(dirPath)
                dirRemovedBool = not fU.exists(dirPath)
                logger.info("removal status %r for dirPath %s", dirRemovedBool, dirPath)
                if not dirRemovedBool:
                    logger.error("unable to remove dirPath %s after compression", dirPath)
                success = ok and dirRemovedBool
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="Directory compression fails with %s" % str(e))
    #
    if not success:
        if dirExistsCheck is False:
            raise HTTPException(status_code=404, detail="Requested directory does not exist %s" % dirPath)
        if dirRemovedBool is False:
            raise HTTPException(status_code=400, detail="Failed to remove directory after compression %s" % dirPath)
        else:
            raise HTTPException(status_code=400, detail="Failed to compress directory")
    else:
        ret = {"success": success, "dirPath": dirPath, "compressPath": compressPath, "statusCode": 200, "statusMessage": "Directory compressed"}

    return ret


@router.post("/compress-dirpath", response_model=CompressResult)
async def compressDirPath(
    dirPath: str = Query(title="File directory", description="File directory", example="/non_standard/directory/"),
):
    """Compress directory at given dirPath, as opposed to standard input paramaters.
    """
    success = False
    compressPath = None
    dirExistsCheck = None
    dirRemovedBool = None
    try:
        fU = FileUtil()
        #
        logger.info("Compressing dirPath %r", dirPath)
        dirExistsCheck = fU.exists(dirPath)
        if dirExistsCheck:
            compressPath = os.path.abspath(dirPath) + ".tar.gz"
            ok = fU.bundleTarfile(compressPath, [os.path.abspath(dirPath)])
            if ok:
                logger.info("created compressPath %s from dirPath %s", compressPath, dirPath)
                fU.remove(dirPath)
                dirRemovedBool = not fU.exists(dirPath)
                logger.info("removal status %r for dirPath %s", dirRemovedBool, dirPath)
                if not dirRemovedBool:
                    logger.error("unable to remove dirPath %s after compression", dirPath)
                success = ok and dirRemovedBool
        #
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        raise HTTPException(status_code=400, detail="Directory compression fails with %s" % str(e))
    #
    if not success:
        if dirExistsCheck is False:
            raise HTTPException(status_code=404, detail="Requested directory does not exist %s" % dirPath)
        if dirRemovedBool is False:
            raise HTTPException(status_code=400, detail="Failed to remove directory after compression %s" % dirPath)
        else:
            raise HTTPException(status_code=400, detail="Failed to compress directory")
    else:
        ret = {"success": success, "dirPath": dirPath, "compressPath": compressPath, "statusCode": 200, "statusMessage": "Directory compressed"}

    return ret
