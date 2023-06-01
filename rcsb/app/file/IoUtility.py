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
copy file, copy dir, move file, compress dir, compress dir path
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import shutil
import logging
import os
import typing
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.Definitions import Definitions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()


class IoUtility:
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
        except ValueError as e:
            logger.exception("Failing with %s", str(e))
            raise ValueError("File copy fails with %s" % str(e))

    async def copyDir(
        self,
        repositoryTypeSource: str,
        depIdSource: str,
        #
        repositoryTypeTarget: str,
        depIdTarget: str,
    ):
        try:
            source_path = PathProvider().getDirPath(repositoryTypeSource, depIdSource)
            if not source_path or not os.path.exists(source_path):
                logger.error(
                    "error - source path does not exist for %s %s",
                    repositoryTypeSource,
                    depIdSource,
                )
                raise FileNotFoundError("Error - source path does not exist")
            target_path = PathProvider().getDirPath(repositoryTypeTarget, depIdTarget)
            logger.info("copying %s to %s", source_path, target_path)
            shutil.copytree(source_path, target_path)
        except FileNotFoundError as exc:
            logger.exception("failing with %s", str(exc))
            raise FileNotFoundError("copy fails with %s" % str(exc))

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
    ):
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
                    raise FileExistsError("Error - file already exists")
                else:
                    logger.info("removing %s", filePathTarget)
                    os.unlink(filePathTarget)
            shutil.move(filePathSource, filePathTarget)
        except ValueError as e:
            logger.exception("Failing with %s", str(e))
            raise OSError("Failing with %s" % str(e))
        except FileExistsError as e:
            logger.exception("Failing with %s", str(e))
            raise OSError("Failing with %s" % str(e))

    async def compressDir(self, repositoryType: str, depId: str):
        try:
            dirPath = self.__pathP.getDirPath(repositoryType, depId)
            if os.path.exists(dirPath):
                compressPath = os.path.abspath(dirPath) + ".tar.gz"
                if FileUtil().bundleTarfile(compressPath, [os.path.abspath(dirPath)]):
                    logger.info(
                        "created compressPath %s from dirPath %s", compressPath, dirPath
                    )
                    shutil.rmtree(dirPath)
                    if os.path.exists(dirPath):
                        logger.error(
                            "unable to remove dirPath %s after compression", dirPath
                        )
                        raise OSError(
                            "Failed to remove directory after compression %s" % dirPath
                        )
            else:
                raise OSError("Requested directory does not exist %s" % dirPath)
        except OSError as e:
            logger.exception("Failing with %s", str(e))
            raise OSError("Directory compression fails with %s" % str(e))

    async def compressDirPath(self, dirPath: str):
        """Compress directory at given dirPath, as opposed to standard input parameters."""
        try:
            if os.path.exists(dirPath):
                compressPath = os.path.abspath(dirPath) + ".tar.gz"
                if FileUtil().bundleTarfile(compressPath, [os.path.abspath(dirPath)]):
                    logger.info(
                        "created compressPath %s from dirPath %s", compressPath, dirPath
                    )
                    shutil.rmtree(dirPath)
                    if os.path.exists(dirPath):
                        logger.error(
                            "unable to remove dirPath %s after compression", dirPath
                        )
                else:
                    raise OSError("Failed to compress directory")
            else:
                raise OSError("Requested directory does not exist %s" % dirPath)
        except OSError as e:
            logger.exception("Failing with %s", str(e))
            raise OSError("Directory compression fails with %s" % str(e))
