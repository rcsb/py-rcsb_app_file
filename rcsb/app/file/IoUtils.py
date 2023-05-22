##
# File:    IoUtils.py
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
from rcsb.app.file.PathUtils import PathUtils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()


class IoUtils:
    """Collected utilities request I/O processing.

    Copy file
    Move file
    """

    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.__cP = cP
        self.__pathU = PathUtils(self.__cP)

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
            filePathSource = self.__pathU.getVersionedPath(
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
            filePathTarget = self.__pathU.getVersionedPath(
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
    ) -> dict:
        """Move a file given standard input parameters for both the source and destination of the file."""
        ret = {"success": True, "statusCode": 200, "statusMessage": "File move success"}
        try:
            filePathSource = self.__pathU.getVersionedPath(
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
            filePathTarget = self.__pathU.getVersionedPath(
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
            os.makedirs(filePathTarget)
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
