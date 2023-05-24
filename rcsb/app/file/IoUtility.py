##
# File:    IoUtility.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates: James Smith
#
"""
Collected I/O utilities.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import shutil
import logging
import os
import typing

from fastapi import HTTPException
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.Definitions import Definitions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()


class IoUtility:
    """Collected utilities request I/O processing."""

    def __init__(self):
        self.__pathP = PathProvider()
        self.__cP = ConfigProvider()
        self.__dP = Definitions()
        self.__repositoryDirPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__sessionDirPath = self.__cP.get("SESSION_DIR_PATH")
        self.__sharedLockDirPath = self.__cP.get("SHARED_LOCK_PATH")
        self.__milestoneList = self.__dP.milestoneList
        self.__repoTypeList = self.__dP.repoTypeList
        self.__contentTypeInfoD = self.__dP.contentTypeD
        self.__fileFormatExtensionD = self.__dP.fileFormatExtD

    def filePath(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: typing.Optional[str],
        partNumber: int,
        contentFormat: str,
        version: str,
    ):
        return self.__pathP.getVersionedPath()

    def checkContentTypeFormat(
        self, contentType: str = None, contentFormat: str = None
    ) -> bool:
        ok = False
        try:
            if (not contentType) and (not contentFormat):
                logger.info("No 'contentType' and 'contentFormat' defined.")
            #
            elif contentType:
                if contentType in self.__contentTypeInfoD:
                    if contentFormat:
                        if contentFormat in self.__contentTypeInfoD[contentType][0]:
                            ok = True
                        else:
                            logger.info(
                                "System does not support %s contentType with %s contentFormat.",
                                contentType,
                                contentFormat,
                            )
                    else:
                        ok = True
                else:
                    logger.info("System does not support %s contentType.", contentType)
                #
            elif contentFormat:
                if contentFormat in self.__fileFormatExtensionD:
                    ok = True
                else:
                    logger.info(
                        "System does not support %s contentFormat.", contentFormat
                    )
                #
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            ok = False
        return ok
        #

    def getMimeType(self, contentFormat: str) -> str:
        cFormat = contentFormat
        if (
            self.__fileFormatExtensionD
            and (contentFormat in self.__fileFormatExtensionD)
            and self.__fileFormatExtensionD[contentFormat]
        ):
            cFormat = self.__fileFormatExtensionD[contentFormat]
        #
        if cFormat in ["cif"]:
            mt = "chemical/x-mmcif"
        elif cFormat in ["pdf"]:
            mt = "application/pdf"
        elif cFormat in ["xml"]:
            mt = "application/xml"
        elif cFormat in ["json"]:
            mt = "application/json"
        elif cFormat in ["txt"]:
            mt = "text/plain"
        elif cFormat in ["pic"]:
            mt = "application/python-pickle"
        else:
            mt = "text/plain"
        #
        return mt

    async def listDir(self, repositoryType: str, depId: str):
        dirList = []
        logger.info(
            "Listing dirPath for repositoryType %r depId %r", repositoryType, depId
        )
        try:
            dirPath = self.__pathP.getDirPath(repositoryType, depId)
            if not os.path.exists(dirPath):
                raise HTTPException(
                    status_code=404, detail=f"Folder not found {dirPath}"
                )
            dirList = os.listdir(dirPath)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            raise HTTPException(
                status_code=404, detail="Directory listing fails with %s" % str(e)
            )
        return {"dirList": dirList}

    async def copyFile(
        self,
        repositoryTypeSource: str,
        depIdSource: str,
        contentTypeSource: str,
        milestoneSource: str,
        partNumberSource: int,
        contentFormatSource: str,
        versionSource: str,
        #
        repositoryTypeTarget: str,
        depIdTarget: str,
        contentTypeTarget: str,
        milestoneTarget: str,
        partNumberTarget: int,
        contentFormatTarget: str,
        versionTarget: str,
    ) -> dict:
        """Copy a file given standard input parameters for both the source and destination of the file."""
        ret = {"success": True, "statusCode": 200, "statusMessage": "File copy success"}
        try:
            filePathSource = self.__pathP.getVersionedPath(
                repositoryTypeSource,
                depIdSource,
                contentTypeSource,
                milestoneSource,
                partNumberSource,
                contentFormatSource,
                versionSource,
            )
            if not versionTarget:
                sourceFileEnd = filePathSource.split(".")[-1]
                if "V" in sourceFileEnd:
                    # set target version to the same as source version
                    versionTarget = sourceFileEnd.split("V")[1]
            filePathTarget = self.__pathP.getVersionedPath(
                repositoryTypeTarget,
                depIdTarget,
                contentTypeTarget,
                milestoneTarget,
                partNumberTarget,
                contentFormatTarget,
                versionTarget,
            )
            if not filePathSource or not filePathTarget:
                raise ValueError(
                    "Source (%r) or target (%r) filepath not defined"
                    % (filePathSource, filePathTarget)
                )
            if not os.path.exists(os.path.dirname(filePathTarget)):
                os.makedirs(os.path.dirname(filePathTarget))
            shutil.copy(filePathSource, filePathTarget)
            ret["filePathSource"] = filePathSource
            ret["filePathTarget"] = filePathTarget
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            ret = {
                "success": False,
                "statusCode": 400,
                "statusMessage": "File copy failed",
            }
            raise HTTPException(
                status_code=400, detail="File checking fails with %s" % str(e)
            )
        return ret

    async def moveFile(
        self,
        repositoryTypeSource: str,
        depIdSource: str,
        contentTypeSource: str,
        milestoneSource: str,
        partNumberSource: int,
        contentFormatSource: str,
        versionSource: str,
        #
        repositoryTypeTarget: str,
        depIdTarget: str,
        contentTypeTarget: str,
        milestoneTarget: str,
        partNumberTarget: int,
        contentFormatTarget: str,
        versionTarget: str,
        #
        overwrite: bool,
    ) -> dict:
        """Move a file given standard input parameters for both the source and destination of the file."""
        ret = {"success": True, "statusCode": 200, "statusMessage": "File move success"}
        try:
            filePathSource = self.__pathP.getVersionedPath(
                repositoryTypeSource,
                depIdSource,
                contentTypeSource,
                milestoneSource,
                partNumberSource,
                contentFormatSource,
                versionSource,
            )
            if not versionTarget:
                sourceFileEnd = filePathSource.split(".")[-1]
                if "V" in sourceFileEnd:
                    # set target version to the same as source version
                    versionTarget = sourceFileEnd.split("V")[1]
            filePathTarget = self.__pathP.getVersionedPath(
                repositoryTypeTarget,
                depIdTarget,
                contentTypeTarget,
                milestoneTarget,
                partNumberTarget,
                contentFormatTarget,
                versionTarget,
            )
            if not filePathSource or not filePathTarget:
                raise ValueError(
                    "Source (%r) or target (%r) filepath not defined"
                    % (filePathSource, filePathTarget)
                )
            if not os.path.exists(os.path.dirname(filePathTarget)):
                os.makedirs(os.path.dirname(filePathTarget))
            if os.path.exists(filePathTarget):
                if not overwrite:
                    raise HTTPException(
                        status_code=403, detail="Error - file already exists"
                    )
                else:
                    logger.info(f"removing {filePathTarget}")
                    os.unlink(filePathTarget)
            shutil.move(filePathSource, filePathTarget)
            ret["filePathSource"] = filePathSource
            ret["filePathTarget"] = filePathTarget
        except Exception as e:
            ret = {
                "success": False,
                "statusCode": 400,
                "statusMessage": "File move failed",
            }
            logger.exception("Failing with %s", str(e))
            raise HTTPException(
                status_code=400, detail="File checking fails with %s" % str(e)
            )
        return ret
