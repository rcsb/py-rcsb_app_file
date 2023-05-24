import typing
import logging
import os
import glob
from rcsb.app.file.Definitions import Definitions
from rcsb.app.file.ConfigProvider import ConfigProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class PathProvider(object):
    def __init__(self):
        self.__cP = ConfigProvider()
        self.__dP = Definitions()
        self.__repositoryDirPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__sessionDirPath = self.__cP.get("SESSION_DIR_PATH")
        self.__sharedLockDirPath = self.__cP.get("SHARED_LOCK_PATH")
        self.__milestoneList = self.__dP.milestoneList
        self.__repoTypeList = self.__dP.repoTypeList
        self.__contentTypeInfoD = self.__dP.contentTypeD
        self.__fileFormatExtensionD = self.__dP.fileFormatExtD

    # path to repository directory / repository type (deposit, archive...)
    def getRepositoryDirPath(self, repositoryType: str) -> typing.Optional[str]:
        if not repositoryType.lower() in self.__repoTypeList:
            return None
        repositoryType = repositoryType.lower()
        repositoryType = repositoryType.replace("onedep-", "")
        return os.path.join(self.__repositoryDirPath, repositoryType)

    # path to repository directory / repository type / deposit id (e.g. D_000)
    def getDirPath(self, repositoryType: str, depId: str) -> typing.Optional[str]:
        dirPath = None
        try:
            repoPath = self.getRepositoryDirPath(repositoryType)
            dirPath = os.path.join(repoPath, depId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return dirPath

    # path to repository directory / repository type / deposit id / file name (version provided)
    def getFilePath(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: str,
        contentFormat: str,
        version: str,
    ):
        path = None
        try:
            path = self.getVersionedPath(
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return path

    # path to repository directory / repo type / dep id / file name (version optionally provided)
    def getVersionedPath(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: str = "1",
        contentFormat: str = "pdbx",
        version: str = "next",
    ) -> typing.Optional[str]:
        fTupL = []
        filePath = None
        filePattern = None
        try:
            repoPath = self.getRepositoryDirPath(repositoryType)
            fnBase = (
                self.__getBaseFileName(
                    depId, contentType, milestone, partNumber, contentFormat
                )
                + ".V"
            )
            filePattern = os.path.join(repoPath, depId, fnBase)
            if str(version).isdigit():
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

    # file name with version provided
    def getFileName(
        self,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: str,
        contentFormat: str,
        version: str,
    ):
        return "%s.V%s" % (
            self.__getBaseFileName(
                depId, contentType, milestone, partNumber, contentFormat
            ),
            version,
        )

    # file name without version
    def getBaseFileName(
        self,
        depId: str = None,
        contentType: str = "model",
        milestone: typing.Optional[str] = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
    ) -> str:
        return self.__getBaseFileName(
            depId, contentType, milestone, partNumber, contentFormat
        )

    # file name without version
    def __getBaseFileName(
        self,
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
    ) -> str:
        return f"{depId}_{self.__contentTypeInfoD[contentType][1]}{self.__validateMilestone(milestone)}_P{partNumber}.{self.__fileFormatExtensionD[contentFormat]}"

    def __validateMilestone(self, milestone):
        """

        Args:
            milestone: str or None

        Returns:
            "-" + str, or blank string

        """
        if milestone and milestone in self.__milestoneList:
            return "-" + milestone
        return ""

    def getFileLockPath(
        self,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
    ) -> str:
        lockPath = self.getSharedLockDirPath()
        fnBase = self.__getBaseFileName(
            depId, contentType, milestone, partNumber, contentFormat
        )
        return os.path.join(lockPath, fnBase + ".lock")

    def getSessionDirPath(self) -> str:
        return self.__sessionDirPath

    def getSharedLockDirPath(self) -> str:
        return self.__sharedLockDirPath
