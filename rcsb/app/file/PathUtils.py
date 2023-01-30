##
# File:    PathUtils.py
# Author:  jdw
# Date:    25-Aug-2021
# Version: 0.001
#
# Updates: James Smith 2023
##
"""
Collected utilities for file system path and file name access.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import glob
import logging
import os
import typing

from rcsb.app.file.ConfigProvider import ConfigProvider

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PathUtils:
    """Collected utilities for file system path and file name access."""

    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.__cP = cP
        self.__repositoryDirPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__sessionDirPath = self.__cP.get("SESSION_DIR_PATH")
        self.__sharedLockDirPath = self.__cP.get("SHARED_LOCK_PATH")
        self.__contentTypeInfoD = self.__cP.get("CONTENT_TYPE")
        self.__fileFormatExtensionD = self.__cP.get("FILE_FORMAT_EXTENSION")
        self.__milestoneList = self.__cP.get("MILESTONE_LIST")

    def getSessionDirPath(self) -> str:
        return self.__sessionDirPath

    def getSharedLockDirPath(self) -> str:
        return self.__sharedLockDirPath

    def getRepositoryDirPath(self, repositoryType: str) -> typing.Optional[str]:
        if repositoryType.lower() in ["onedep-archive", "archive"]:
            return os.path.join(self.__repositoryDirPath, "archive")
        elif repositoryType.lower() in ["onedep-deposit", "deposit"]:
            return os.path.join(self.__repositoryDirPath, "deposit")
        elif repositoryType.lower() in ["onedep-session", "session"]:
            return os.path.join(self.__repositoryDirPath, "session")
        elif repositoryType.lower() in ["onedep-workflow", "workflow"]:
            return os.path.join(self.__repositoryDirPath, "workflow")
        return None

    def getFileLockPath(self, depId: str, contentType: str, milestone: str, partNumber: int, contentFormat: str) -> str:
        lockPath = self.getSharedLockDirPath()
        fnBase = self.__getBaseFileName(depId, contentType, milestone, partNumber, contentFormat)
        return os.path.join(lockPath, fnBase + ".lock")

    def getSliceFilePath(self, sessionId: str, sliceIndex: int, sliceTotal: int) -> str:
        sessionPath = self.getSessionDirPath()
        fnBase = f"{sessionId}_{sliceIndex}.{sliceTotal}"
        return os.path.join(sessionPath, sessionId, fnBase)

    def getSliceLockPath(self, sessionId: str, sliceIndex: int, sliceTotal: int) -> str:
        lockPath = self.getSharedLockDirPath()
        fnBase = f"{sessionId}_{sliceIndex}.{sliceTotal}"
        return os.path.join(lockPath, fnBase + ".lock")

    def getVersionedPath(self,
                         repositoryType: str = "archive",
                         depId: str = None,
                         contentType: str = "model",
                         milestone: str = None,
                         partNumber: str = "1",
                         contentFormat: str = "pdbx",
                         version: str = "next"
                         ) -> typing.Optional[str]:
        fTupL = []
        filePath = None
        filePattern = None
        try:
            repoPath = self.getRepositoryDirPath(repositoryType)
            fnBase = self.__getBaseFileName(depId, contentType, milestone, partNumber, contentFormat) + ".V"
            filePattern = os.path.join(repoPath, depId, fnBase)
            if version.isdigit():
                filePath = filePattern + str(version)
            else:
                for pth in glob.iglob(filePattern + "*"):
                    vNo = int(pth.split(".")[-1][1:])
                    fTupL.append((pth, vNo))
                # - sort in decending version order -
                if len(fTupL) > 1:
                    fTupL.sort(key=lambda tup: tup[1], reverse=True)
                #
                if version.lower() == "next":
                    if fTupL:
                        filePath = filePattern + str(fTupL[0][1] + 1)
                    else:
                        filePath = filePattern + str(1)
                    #
                elif version.lower() in ["last", "latest"]:
                    if fTupL:
                        filePath = fTupL[0][0]
                    #
                elif version.lower() in ["prev", "previous"]:
                    if len(fTupL) > 1:
                        filePath = fTupL[1][0]
                    #
                elif version.lower() in ["first"]:
                    if fTupL:
                        filePath = fTupL[-1][0]
                    #
                elif version.lower() in ["second"]:
                    if len(fTupL) > 1:
                        filePath = fTupL[-2][0]
                    #
                #
            #
        except Exception as e:
            logger.info(filePattern)
            logger.exception("Failing with %s", str(e))
        return filePath

    def getDirPath(self, repositoryType: str, depId: str) -> typing.Optional[str]:
        dirPath = None
        try:
            repoPath = self.getRepositoryDirPath(repositoryType)
            dirPath = os.path.join(repoPath, depId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return dirPath

    def checkContentTypeFormat(self, contentType: str = None, contentFormat: str = None) -> bool:
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
                            logger.info("System does not support %s contentType with %s contentFormat.", contentType, contentFormat)
                    else:
                        ok = True
                else:
                    logger.info("System does not support %s contentType.", contentType)
                #
            elif contentFormat:
                if contentFormat in self.__fileFormatExtensionD:
                    ok = True
                else:
                    logger.info("System does not support %s contentFormat.", contentFormat)
                #
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            ok = False
        return ok
        #

    def getMimeType(self, contentFormat: str) -> str:
        cFormat = contentFormat
        if self.__fileFormatExtensionD and (contentFormat in self.__fileFormatExtensionD) and self.__fileFormatExtensionD[contentFormat]:
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

    def __validateMilestone(self, milestone):
        """

        Args:
            milestone: str or None

        Returns:
            "-" + str, or blank string

        """
        if milestone and milestone.lstrip().rstrip() != "" and milestone.lower().lstrip().rstrip() != "none" and milestone.lower().lstrip().rstrip() != "null":
            if milestone in self.__milestoneList:
                return '-' + milestone
        return ""

    def getBaseFileName(self,
                        depId: str = None,
                        contentType: str = "model",
                        milestone: typing.Optional[str] = None,
                        partNumber: int = 1,
                        contentFormat: str = "pdbx"
                        ) -> str:
        return self.__getBaseFileName(depId, contentType, milestone, partNumber, contentFormat)

    def __getBaseFileName(self,
                          depId: str = None,
                          contentType: str = "model",
                          milestone: str = None,
                          partNumber: int = 1,
                          contentFormat: str = "pdbx"
                          ) -> str:
        return f"{depId}_{self.__contentTypeInfoD[contentType][1]}{self.__validateMilestone(milestone)}_P{partNumber}.{self.__fileFormatExtensionD[contentFormat]}"
