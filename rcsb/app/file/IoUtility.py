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
check hash, get hash digest, copy file, copy dir, move file, compress dir, compress dir path, decompress dir
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import shutil
import logging
import os
import typing
import hashlib
from fastapi import HTTPException
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.ConfigProvider import ConfigProvider


provider = ConfigProvider()
locktype = provider.get("LOCK_TYPE")
kvmode = provider.get("KV_MODE")
if locktype == "redis":
    if kvmode == "redis":
        from rcsb.app.file.RedisLock import Locking
    else:
        from rcsb.app.file.RedisSqliteLock import Locking
elif locktype == "ternary":
    from rcsb.app.file.TernaryLock import Locking
else:
    from rcsb.app.file.SoftLock import Locking


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()


class IoUtility(object):
    def __init__(self, pP=None):
        self.__pP = pP if pP else PathProvider()

    # does not lock hash transactions, allows invoker to choose whether to lock
    def checkHash(self, pth: str, hashDigest: str, hashType: str) -> bool:
        tHash = self.getHashDigest(pth, hashType)
        return tHash == hashDigest

    def getHashDigest(
        self, filePath: str, hashType: str = "MD5", blockSize: int = 65536
    ) -> typing.Optional[str]:
        if hashType not in ["MD5", "SHA1", "SHA256"]:
            return None
        try:
            if hashType == "SHA1":
                hashObj = hashlib.sha1()
            elif hashType == "SHA256":
                hashObj = hashlib.sha256()
            elif hashType == "MD5":
                hashObj = hashlib.md5()
            # hash file
            if os.path.exists(filePath):
                with open(filePath, "rb") as r:
                    # for block in iter(lambda: r.read(blockSize), b""):
                    while block := r.read(blockSize):
                        hashObj.update(block)
                return hashObj.hexdigest()
        except Exception as e:
            logger.exception("Failing with file %s %r", filePath, str(e))
        return None

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
        #
        overwrite: bool,
    ):
        filePathSource = self.__pP.getVersionedPath(
            repositoryTypeSource,
            depIdSource,
            contentTypeSource,
            milestoneSource,
            partNumberSource,
            contentFormatSource,
            versionSource,
        )
        filePathTarget = self.__pP.getVersionedPath(
            repositoryTypeTarget,
            depIdTarget,
            contentTypeTarget,
            milestoneTarget,
            partNumberTarget,
            contentFormatTarget,
            versionTarget,
        )
        if not filePathSource or not filePathTarget:
            raise HTTPException(
                status_code=400, detail="error - source or target filepath not defined"
            )
        if not os.path.exists(filePathSource):
            raise HTTPException(
                status_code=404, detail="error - file not found %s" % filePathSource
            )
        if os.path.exists(filePathTarget) and not overwrite:
            raise HTTPException(
                status_code=403,
                detail="error - file already exists %s" % filePathTarget,
            )
        if not os.path.exists(os.path.dirname(filePathTarget)):
            os.makedirs(os.path.dirname(filePathTarget))
        logging.info("copying %s to %s", filePathSource, filePathTarget)
        try:
            async with Locking(filePathSource, "r"):
                shutil.copy(filePathSource, filePathTarget)
        except (FileExistsError, OSError) as err:
            raise HTTPException(status_code=400, detail="error %r" % err)

    async def copyDir(
        self,
        repositoryTypeSource: str,
        depIdSource: str,
        #
        repositoryTypeTarget: str,
        depIdTarget: str,
        #
        overwrite: bool,
    ):
        source_path = self.__pP.getDirPath(repositoryTypeSource, depIdSource)
        target_path = self.__pP.getDirPath(repositoryTypeTarget, depIdTarget)
        if not source_path or not target_path:
            raise HTTPException(
                status_code=400, detail="error - source path or target path not defined"
            )
        if not os.path.exists(source_path):
            raise HTTPException(
                status_code=404, detail="error - source path does not exist"
            )
        if os.path.exists(target_path):
            if not overwrite:
                raise HTTPException(
                    status_code=403,
                    detail="error - directory already exists %s" % target_path,
                )
            else:
                shutil.rmtree(target_path)
        logger.info("copying %s to %s", source_path, target_path)
        try:
            async with Locking(source_path, "r", is_dir=True):
                shutil.copytree(source_path, target_path)
        except (FileExistsError, OSError) as err:
            raise HTTPException(status_code=400, detail="error %r" % err)

    async def makeDirs(self, repositoryType: str, depId: str):
        # does not require pre-existence of repository type directory
        if repositoryType not in self.__pP.repoTypeList:
            raise HTTPException(
                status_code=400,
                detail="error - unknown repository type %s" % repositoryType,
            )
        target_path = self.__pP.getDirPath(repositoryType, depId)
        if not target_path:
            raise HTTPException(status_code=400, detail="error - path not well formed")
        if os.path.exists(target_path):
            raise HTTPException(
                status_code=403, detail="error - path already exists %s" % target_path
            )
        logger.info("making directories %s", target_path)
        try:
            os.makedirs(target_path)
        except Exception as err:
            raise HTTPException(status_code=400, detail="error %r" % err)

    async def makeDir(self, repositoryType: str, depId: str):
        # requires pre-existence of repository type directory
        if repositoryType not in self.__pP.repoTypeList:
            raise HTTPException(
                status_code=400,
                detail="error - unknown repository type %s" % repositoryType,
            )
        target_path = self.__pP.getDirPath(repositoryType, depId)
        if not target_path:
            raise HTTPException(status_code=400, detail="error - path not well formed")
        dirname = os.path.dirname(target_path)
        if not os.path.exists(dirname):
            logging.exception("error - directory does not exist %s", dirname)
            raise HTTPException(
                status_code=404,
                detail="error - %s directory does not exist" % repositoryType,
            )
        if os.path.exists(target_path):
            raise HTTPException(
                status_code=403, detail="error - path already exists %s" % target_path
            )
        logger.info("making directory %s", target_path)
        try:
            os.mkdir(target_path)
        except Exception as err:
            raise HTTPException(status_code=400, detail="error %r" % err)

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
        filePathSource = self.__pP.getVersionedPath(
            repositoryTypeSource,
            depIdSource,
            contentTypeSource,
            milestoneSource,
            partNumberSource,
            contentFormatSource,
            versionSource,
        )
        filePathTarget = self.__pP.getVersionedPath(
            repositoryTypeTarget,
            depIdTarget,
            contentTypeTarget,
            milestoneTarget,
            partNumberTarget,
            contentFormatTarget,
            versionTarget,
        )
        if not filePathSource or not filePathTarget:
            raise HTTPException(
                status_code=400, detail="Source or target filepath not defined"
            )
        if not os.path.exists(filePathSource):
            raise HTTPException(
                status_code=404,
                detail="error - file does not exist %s" % filePathSource,
            )
        if os.path.exists(filePathTarget):
            if not overwrite:
                raise HTTPException(
                    status_code=403,
                    detail="error - file already exists %s" % filePathTarget,
                )
            else:
                logger.info("removing %s", filePathTarget)
                os.unlink(filePathTarget)
        if not os.path.exists(os.path.dirname(filePathTarget)):
            os.makedirs(os.path.dirname(filePathTarget))
        logger.info("moving %s to %s", filePathSource, filePathTarget)
        try:
            async with Locking(filePathSource, "w"):
                shutil.move(filePathSource, filePathTarget)
        except (FileExistsError, OSError) as err:
            raise HTTPException(status_code=400, detail="error %r" % err)

    async def compressDir(self, repositoryType: str, depId: str):
        # removes uncompressed source afterward
        dirPath = self.__pP.getDirPath(repositoryType, depId)
        if not dirPath:
            raise HTTPException(
                status_code=400, detail="error - could not form dir path"
            )
        if not os.path.exists(dirPath):
            raise HTTPException(
                status_code=404, detail="error - path not found %s" % dirPath
            )
        compressPath = os.path.abspath(dirPath) + ".tar.gz"
        if os.path.exists(compressPath):
            raise HTTPException(
                status_code=403,
                detail="error - requested path already exists %s" % compressPath,
            )
        try:
            async with Locking(dirPath, "w", is_dir=True):
                if FileUtil().bundleTarfile(compressPath, [os.path.abspath(dirPath)]):
                    shutil.rmtree(dirPath)
                    if os.path.exists(dirPath):
                        logger.error(
                            "unable to remove dirPath %s after compression", dirPath
                        )
        except (FileExistsError, OSError) as err:
            raise HTTPException(status_code=400, detail="error %r" % err)

    async def compressDirPath(self, dirPath: str):
        """
        Compress directory at given dirPath, as opposed to standard input parameters.
        removes uncompressed source afterward
        """
        if not os.path.exists(dirPath):
            raise HTTPException(
                status_code=404, detail="error - path not found %s" % dirPath
            )
        compressPath = os.path.abspath(dirPath) + ".tar.gz"
        if os.path.exists(compressPath):
            raise HTTPException(
                status_code=403,
                detail="error - requested path already exists %s" % compressPath,
            )
        try:
            async with Locking(dirPath, "w", is_dir=True):
                if FileUtil().bundleTarfile(compressPath, [os.path.abspath(dirPath)]):
                    shutil.rmtree(dirPath)
                    if os.path.exists(dirPath):
                        logger.error(
                            "unable to remove dirPath %s after compression", dirPath
                        )
                else:
                    raise HTTPException(
                        status_code=400, detail="error - failed to compress directory"
                    )
        except (FileExistsError, OSError) as err:
            raise HTTPException(status_code=400, detail="error %r" % err)

    async def decompressDir(self, repositoryType: str, depId: str):
        # removes compressed source afterward
        dirPath = self.__pP.getDirPath(repositoryType, depId)
        if not dirPath:
            raise HTTPException(
                status_code=400, detail="error - could not form dir path"
            )
        if os.path.exists(dirPath):
            raise HTTPException(
                status_code=403, detail="error - path already exists %s" % dirPath
            )
        decompressPath = os.path.abspath(dirPath) + ".tar.gz"
        if not os.path.exists(decompressPath):
            raise HTTPException(
                status_code=404, detail="error - path not found %s" % decompressPath
            )
        try:
            async with Locking(decompressPath, "w"):
                if FileUtil().unbundleTarfile(
                    decompressPath, os.path.abspath(os.path.dirname(dirPath))
                ):
                    os.unlink(decompressPath)
                    if os.path.exists(decompressPath):
                        logger.error(
                            "unable to remove dirPath %s after compression", dirPath
                        )
                else:
                    raise HTTPException(
                        status_code=400, detail="error - decompression failed"
                    )
        except (FileExistsError, OSError) as err:
            raise HTTPException(status_code=400, detail="error %r" % err)
