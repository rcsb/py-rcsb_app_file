##
# File:    IoUtility.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates: James Smith 2023
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

    async def copyFile(
        self,
        repositoryTypeSource: str,
        depIdSource: str,
        contentTypeSource: str,
        milestoneSource: typing.Optional[str],
        partNumberSource: int,
        contentFormatSource: str,
        versionSource: str,
        #
        repositoryTypeTarget: str,
        depIdTarget: str,
        contentTypeTarget: str,
        milestoneTarget: typing.Optional[str],
        partNumberTarget: int,
        contentFormatTarget: str,
        versionTarget: str,
    ):
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
            logging.warning("copying %s to %s", filePathSource, filePathTarget)
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

    async def copyDir(
        self,
        repositoryTypeSource: str,
        depIdSource: str,
        #
        repositoryTypeTarget: str,
        depIdTarget: str,
    ):
        ret = {"success": True, "statusCode": 200, "statusMessage": "Dir copy success"}
        try:
            logger.info("copy dir %s %s %s %s", repositoryTypeSource, depIdSource, repositoryTypeTarget, depIdTarget)
            source_path = PathProvider().getDirPath(repositoryTypeSource, depIdSource)
            logger.info("copying dir %s", source_path)
            if not source_path or not os.path.exists(source_path):
                logger.error("error - source path does not exist for %s %s", repositoryTypeSource, depIdSource)
                raise HTTPException(status_code=404, detail="Error - source path does not exist")
            target_path = PathProvider().getDirPath(repositoryTypeTarget, depIdTarget)
            logger.info("copying %s to %s", source_path, target_path)
            shutil.copytree(source_path, target_path)
            ret["dirPathSource"] = source_path
            ret["dirPathTarget"] = target_path
        except HTTPException as exc:
            logger.exception("Failing with %s", str(exc))
            ret = {
                "success": False,
                "statusCode": 404,
                "statusMessage": "source path does not exist"
            }
            raise HTTPException(
                status_code=404, detail=exc.detail
            )
        except Exception as exc:
            logger.exception("Failing with %s", str(exc))
            ret = {
                "success": False,
                "statusCode": 400,
                "statusMessage": "Dir copy failed",
            }
            raise HTTPException(
                status_code=400, detail="Dir checking fails with %s" % str(exc)
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
                    logger.info("removing %s", filePathTarget)
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
