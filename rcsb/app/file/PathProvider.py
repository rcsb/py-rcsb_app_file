# file - PathProvider.py
# author - James Smith 2023

import typing
import logging
import os
import glob
from fastapi import HTTPException
from rcsb.app.file.Definitions import Definitions
from rcsb.app.file.ConfigProvider import ConfigProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# functions -
# get file name, get versioned path, get latest version, get next version, list dir, dir exists, file exists, file size, get file lock path


class PathProvider(object):
    def __init__(self, cP=None):
        self.__cP = cP if cP else ConfigProvider()
        self.__repositoryDirPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__sessionDirPath = self.__cP.get("SESSION_DIR_PATH")
        self.__sharedLockDirPath = self.__cP.get("SHARED_LOCK_PATH")

        self.__dP = Definitions()
        self.__milestoneList = self.__dP.milestoneList
        self.__repoTypeList = self.__dP.repoTypeList
        self.__contentTypeInfoD = self.__dP.contentTypeD
        self.__fileFormatExtensionD = self.__dP.fileFormatExtD

    # functions that find relative paths on server, or file names from parameters
    # does not return absolute paths unless the repository path specified in config.yml is an absolute path
    # primarily for internal use by the file API itself

    # returns relative path to repository directory / repository type (deposit, archive...)
    def getRepositoryDirPath(self, repositoryType: str) -> typing.Optional[str]:
        if not repositoryType.lower() in self.__repoTypeList:
            return None
        repositoryType = repositoryType.lower()
        repositoryType = repositoryType.replace("onedep-", "")
        return os.path.join(self.__repositoryDirPath, repositoryType)

    # returns relative path to repository directory / repository type / deposit id (e.g. D_000)
    def getDirPath(self, repositoryType: str, depId: str) -> typing.Optional[str]:
        dirPath = None
        try:
            repoPath = self.getRepositoryDirPath(repositoryType)
            if repoPath:
                dirPath = os.path.join(repoPath, depId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return dirPath

    # returns relative path to repository directory / repository type / deposit id / file name
    # does not test file existence
    def getVersionedPath(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: str,
        contentFormat: str,
        version: str,
    ) -> typing.Optional[str]:
        path = None
        try:
            repoPath = self.getRepositoryDirPath(repositoryType)
            if repoPath:
                fileName = (
                    self.getBaseFileName(
                        depId, contentType, milestone, partNumber, contentFormat
                    )
                    + ".V"
                )
                if fileName:
                    filePath = os.path.join(repoPath, depId, fileName)
                    versionNumber = self.getVersion(
                        repositoryType,
                        depId,
                        contentType,
                        milestone,
                        partNumber,
                        contentFormat,
                        version,
                    )
                    if versionNumber:
                        path = "%s%d" % (filePath, versionNumber)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return path

    def getNextVersion(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: str = "1",
        contentFormat: str = "pdbx",
    ) -> typing.Optional[int]:
        version = "next"
        return self.getVersion(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )

    def getLatestVersion(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: str = "1",
        contentFormat: str = "pdbx",
    ) -> typing.Optional[int]:
        version = "latest"
        return self.getVersion(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )

    def getVersion(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: str = "1",
        contentFormat: str = "pdbx",
        version: str = "next",
    ) -> typing.Optional[int]:
        if not str(version).isdigit():
            version = version.lower()
        try:
            if str(version).isdigit():
                return int(version)
            else:
                fTupL = []
                repoPath = self.getRepositoryDirPath(repositoryType)
                if repoPath:
                    fnBase = (
                        self.getBaseFileName(
                            depId, contentType, milestone, partNumber, contentFormat
                        )
                        + ".V"
                    )
                    if fnBase:
                        filePattern = os.path.join(repoPath, depId, fnBase)
                        for pth in glob.iglob(filePattern + "*"):
                            vNo = int(pth.split(".")[-1][1:])
                            fTupL.append((pth, vNo))
                        if len(fTupL) == 0:
                            if version == "next":
                                return 1
                            else:
                                return None
                        elif len(fTupL) == 1:
                            if version in ["first", "last", "latest"]:
                                return 1
                            elif version == "next":
                                return 2
                            else:
                                return None
                        else:
                            # - sort in descending version order -
                            fTupL.sort(key=lambda tup: tup[1], reverse=True)
                            if version == "next":
                                return fTupL[0][1] + 1
                            elif version in ["last", "latest"]:
                                return fTupL[0][1]
                            elif version in ["prev", "previous"]:
                                return fTupL[1][1]
                            elif version == "first":
                                return fTupL[-1][1]
                            elif version == "second":
                                return fTupL[-2][1]
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None

    # returns file name without version
    def getBaseFileName(
        self,
        depId: str = None,
        contentType: str = None,
        milestone: str = "",
        partNumber: int = 1,
        contentFormat: str = None,
    ) -> typing.Optional[str]:
        if not depId or not contentType or not contentFormat:
            return None
        typ = frmt = None
        if contentType in self.__contentTypeInfoD:
            typ = self.__contentTypeInfoD[contentType][1]
        if contentFormat in self.__fileFormatExtensionD:
            frmt = self.__fileFormatExtensionD[contentFormat]
        if not typ or not frmt:
            return None
        mst = self.__validateMilestone(milestone)
        return f"{depId}_{typ}{mst}_P{partNumber}.{frmt}"

    # returns file name with version provided (without changing version)
    # validates numeric version
    # used for downloads
    def getFileName(
        self,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: str,
        contentFormat: str,
        version: int,
    ) -> typing.Optional[str]:
        if str(version).isdigit():
            return "%s.V%s" % (
                self.getBaseFileName(
                    depId, contentType, milestone, partNumber, contentFormat
                ),
                version,
            )
        return None

    # functions that validate parameters that form a file path

    def checkContentTypeFormat(self, contentType: str, contentFormat: str) -> bool:
        # validate content parameters
        if not contentType or not contentFormat:
            return False
        if contentType not in self.__contentTypeInfoD:
            return False
        if contentFormat not in self.__fileFormatExtensionD:
            return False
        # assert valid combination of type and format
        if contentFormat in self.__contentTypeInfoD[contentType][0]:
            return True
        logger.info(
            "System does not support %s contentType with %s contentFormat.",
            contentType,
            contentFormat,
        )
        return False

    def __validateMilestone(self, milestone):
        """

        Args:
            milestone: str or None

        Returns:
            "-" + milestone, or blank string

        """
        if milestone and milestone in self.__milestoneList:
            return "-" + milestone
        return ""

    # functions that just use PathProvider, so they should be placed in same file

    async def listDir(self, repositoryType: str, depId: str) -> list:
        dirPath = self.getDirPath(repositoryType, depId)
        if not dirPath:
            raise HTTPException(status_code=400, detail="could not form directory path")
        if not os.path.exists(dirPath):
            raise HTTPException(status_code=404, detail="file not found")
        if not os.path.isdir(dirPath):
            raise HTTPException(
                status_code=404, detail="path was a file, not a directory"
            )
        dirList = os.listdir(dirPath)
        return dirList

    async def fileSize(
        self,
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    ) -> typing.Optional[int]:
        filePath = self.getVersionedPath(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
        if not filePath or not os.path.exists(filePath):
            raise HTTPException(status_code=404, detail="requested file path not found")
        return os.path.getsize(filePath)

    def fileExists(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
    ) -> bool:
        # existence of file from 1-dep parameters
        filePath = self.getVersionedPath(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
        if not filePath or not os.path.exists(filePath):
            return False
        return True

    def dirExists(self, repositoryType: str, depId: str) -> bool:
        # existence of directory from 1-dep parameters
        dirPath = self.getDirPath(repositoryType, depId)
        if not dirPath or not os.path.exists(dirPath):
            return False
        return True

    # other functions related to paths

    def getFileLockPath(
        self,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
    ) -> str:
        lockPath = self.getSharedLockDirPath()
        fnBase = self.getBaseFileName(
            depId, contentType, milestone, partNumber, contentFormat
        )
        return os.path.join(lockPath, fnBase + ".lock")

    # functions that are not yet used

    def getSessionDirPath(self) -> str:
        return self.__sessionDirPath

    def getSharedLockDirPath(self) -> str:
        return self.__sharedLockDirPath
