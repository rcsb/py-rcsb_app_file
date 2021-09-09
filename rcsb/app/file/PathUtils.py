##
# File:    PathUtils.py
# Author:  jdw
# Date:    25-Aug-2021
# Version: 0.001
#
# Updates:
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

logger = logging.getLogger(__name__)


class PathUtils:
    """Collected utilities for file system path and file name access."""

    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.__cP = cP
        self.__repositoryDirPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__sessionDirPath = self.__cP.get("SESSION_DIR_PATH")
        self.__sharedLockDirPath = self.__cP.get("SHARED_LOCK_PATH")

    def getSessionDirPath(self) -> str:
        return self.__sessionDirPath

    def getSharedLockDirPath(self) -> str:
        return self.__sharedLockDirPath

    def getRepositoryDirPath(self, repositoryType: str) -> typing.Optional[str]:
        if repositoryType.lower() in ["onedep-archive", "archive"]:
            return os.path.join(self.__repositoryDirPath, "archive")
        elif repositoryType.lower() in ["onedep-deposit", "deposit"]:
            return os.path.join(self.__repositoryDirPath, "deposit")
        return None

    def getFileLockPath(self, idCode: str, contentType: str, partNumber: int, contentFormat: str) -> str:
        lockPath = self.getSharedLockDirPath()
        fnBase = f"{idCode}_{contentType}_P{partNumber}.{contentFormat}"
        return os.path.join(lockPath, fnBase + ".lock")

    def getSliceFilePath(self, sessionId: str, sliceIndex: int, sliceTotal: int) -> str:
        sessionPath = self.getSessionDirPath()
        fnBase = f"{sessionId}_{sliceIndex}.{sliceTotal}"
        return os.path.join(sessionPath, sessionId, fnBase)

    def getSliceLockPath(self, sessionId: str, sliceIndex: int, sliceTotal: int) -> str:
        lockPath = self.getSharedLockDirPath()
        fnBase = f"{sessionId}_{sliceIndex}.{sliceTotal}"
        return os.path.join(lockPath, fnBase + ".lock")

    def getVersionedPath(self, repositoryType: str, idCode: str, contentType: str, partNumber: int, contentFormat: str, version: str) -> typing.Optional[str]:
        fTupL = []
        filePath = None
        try:
            repoPath = self.getRepositoryDirPath(repositoryType)
            fnBase = f"{idCode}_{contentType}_P{partNumber}.{contentFormat}.V"
            filePattern = os.path.join(repoPath, idCode, fnBase)
            if version.isdigit():
                filePath = filePattern + str(version)
            else:
                # JDW wrap this for async?
                for pth in glob.iglob(filePattern + "*"):
                    vNo = int(pth.split(".")[-1][1:])
                    fTupL.append((pth, vNo))
                # - sort in decending version order -
                fTupL.sort(key=lambda tup: tup[1], reverse=True)
                if version.lower() == "next":
                    filePath = filePattern + str(fTupL[0][1] + 1)
                elif version.lower() == ["last", "latest"]:
                    filePath = fTupL[0][0]
                elif version.lower() in ["prev", "previous"]:
                    filePath = fTupL[1][0]
                elif version.lower() in ["first"]:
                    filePath = fTupL[-1][0]
                elif version.lower() in ["second"]:
                    filePath = fTupL[-2][0]
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return filePath

    def getMimeType(self, contentFormat: str) -> str:
        if contentFormat in ["cif"]:
            mt = "chemical/x-mmcif"
        elif contentFormat in ["pdf"]:
            mt = "application/pdf"
        elif contentFormat in ["xml"]:
            mt = "application/xml"
        elif contentFormat in ["json"]:
            mt = "application/json"
        elif contentFormat in ["txt"]:
            mt = "text/plain"
        elif contentFormat in ["pic"]:
            mt = "application/python-pickle"
        else:
            mt = "text/plain"

        return mt
