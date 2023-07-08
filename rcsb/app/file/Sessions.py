# file: Sessions.py
# author: James Smith 2023

import asyncio
import datetime
import json
import os
import sys
import time
import logging
import uuid
import typing
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.KvRedis import KvRedis
from rcsb.app.file.KvSqlite import KvSqlite

logging.basicConfig(level=logging.INFO)

# session maintenance for one upload
# includes file locking and optional resumability API
# functions - upload helper functions, database functions, locking functions, session placeholder functions, bulk maintenance function


class Sessions(object):
    # statelessly invoked once per chunk of every upload
    def __init__(self, cP, uploadId, *args):
        self.cP = cP if cP else ConfigProvider()
        self.uploadId = uploadId
        # invoked only once per upload
        if not self.uploadId:
            if args:
                (
                    resumable,
                    repositoryType,
                    depId,
                    contentType,
                    milestone,
                    partNumber,
                    contentFormat,
                    version,
                ) = args
                if resumable:
                    self.uploadId = self.getResumedUpload(
                        repositoryType,
                        depId,
                        contentType,
                        milestone,
                        partNumber,
                        contentFormat,
                        version,
                    )
                    if self.uploadId:
                        logging.info("resuming session %s", self.uploadId)
            if not self.uploadId:
                self.uploadId = self.getNewUploadId()

    # UPLOAD HELPER FUNCTIONS

    def getNewUploadId(self):
        return uuid.uuid4().hex

    # find upload id using file parameters
    # tasks - improve async handling
    # cannot be async when invoked from init
    def getResumedUpload(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
        version: str = "next",
    ):  # returns uploadId or None
        mapKey = self.getKvPrimaryMapKey(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
        )
        if self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        else:
            logging.exception("error - unknown kv mode %s", self.cP.get("KV_MODE"))
            return None
        uploadId = kV.getMap(mapKey)
        if not uploadId:
            # not a resumed upload
            return None
        # remove expired entry
        timestamp = int(kV.getSession(uploadId, "timestamp"))
        now = datetime.datetime.timestamp(datetime.datetime.now(datetime.timezone.utc))
        duration = now - timestamp
        max_duration = self.cP.get("KV_MAX_SECONDS")
        if duration > max_duration:
            logging.info("removing expired entry %s", uploadId)
            asyncio.run(
                self.removeKvExpiredEntry(
                    mapKey=mapKey,
                    depId=depId,
                    repositoryType=repositoryType,
                    kV=kV,
                    uploadId=uploadId,
                )
            )
            return None
        return uploadId

    # in-place temp file name and path
    def getTempFilePath(self, dirPath, uploadId=None):
        if not uploadId:
            uploadId = self.uploadId
        tempPath = os.path.join(dirPath, "._" + uploadId)
        return tempPath

    def getSaveFilePath(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        allowOverwrite: bool,
    ) -> str:
        pP = PathProvider(self.cP)
        if not pP.checkContentTypeFormat(contentType, contentFormat):
            logging.error("Error 400 - bad content type and/or format")
            raise ValueError()
        outPath = pP.getVersionedPath(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
        )
        if os.path.exists(outPath) and not allowOverwrite:
            logging.exception(
                "Error 403 - encountered existing file - overwrite prohibited"
            )
            raise FileExistsError()
        if not outPath:
            logging.error("Error 400 - could not make file path from parameters")
            raise ValueError()
        # return truncated file path to avoid exposing absolute path to client
        repositoryPath = self.cP.get("REPOSITORY_DIR_PATH")
        if outPath.startswith(repositoryPath):
            outPath = outPath.replace(repositoryPath, "")
            outPath = outPath[1:]
        else:
            logging.exception("Error in file path formation %s", outPath)
            raise ValueError()
        return outPath

    # clear temp files, then clear kv session
    async def closeSession(
        self,
        tempPath: str,
        resumable: bool = False,
        mapKey: typing.Optional[str] = None,
        kV=None,
        uid: typing.Optional[str] = None,
    ):
        if os.path.exists(tempPath):
            os.unlink(tempPath)
        self.removePlaceholderFile(tempPath)
        if not resumable:
            return True
        if not uid:
            uid = self.uploadId
        return await self.clearKvSession(mapKey, kV, uid)

    # DATABASE FUNCTIONS (RESUMABLE UPLOADS ONLY)

    # compute chunks uploaded using current file size divided by chunk size
    # parameter dir path = absolute path without file name
    async def getUploadCount(self, dirPath: str) -> int:
        status = await self.getKvSession()
        if status:
            status = str(status)
            status = status.replace("'", '"')
            status = json.loads(status)
            if "chunkSize" in status:
                chunkSize = int(status["chunkSize"])
                tempPath = self.getTempFilePath(dirPath)
                if os.path.exists(tempPath):
                    fileSize = os.path.getsize(tempPath)
                    uploadCount = round(fileSize / chunkSize)
                    return int(uploadCount)
                else:
                    logging.exception("error - could not find path %s", tempPath)
        return 0

    async def hasResumableUpload(self, kV=None, uploadId=None):
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        if not uploadId:
            uploadId = self.uploadId
        return kV.getKey(uploadId, kV.sessionTable) is not None

    # returns entire dictionary of session table entry
    async def getKvSession(self, kV=None, uploadId=None):
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        if not uploadId:
            uploadId = self.uploadId
        return kV.getKey(uploadId, kV.sessionTable)

    async def setKvSession(self, key1, key2, val):
        if self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        kV.setSession(key1, key2, val)

    async def setKvMap(self, key, val):
        if self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        kV.setMap(key, val)

    def getKvPrimaryMapKey(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
        version: str = "next",
    ):
        pP = PathProvider(self.cP)
        filename = pP.getVersionedPath(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
        )
        if not filename:
            return None
        filename = os.path.basename(filename)
        filename = repositoryType + "_" + filename
        return filename

    def getKvPreparedMapKey(self, repositoryType, versionedPath):
        # when versioned path is already found
        if not versionedPath:
            return None
        filename = os.path.basename(versionedPath)
        filename = repositoryType + "_" + filename
        return filename

    # remove an entry from session table and map table, remove corresponding hidden files
    # does not check expiration
    async def removeKvExpiredEntry(
        self,
        mapKey: str = None,
        depId: str = None,
        repositoryType: str = None,
        kV=None,
        uploadId: str = None,
    ):
        if not uploadId:
            uploadId = self.uploadId
        pP = PathProvider(self.cP)
        dirPath = pP.getDirPath(repositoryType, depId)
        tempPath = self.getTempFilePath(dirPath, uploadId)
        resumable = True
        await self.closeSession(tempPath, resumable, mapKey, kV)

    # clear one entry from session table
    async def clearKvUploadId(self, kV=None, uid=None):
        if not uid:
            uid = self.uploadId
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        response = None
        try:
            response = kV.clearSessionKey(uid)
        except Exception:
            return False
        return response

    # clear one entry from session table and corresponding entry from map table
    async def clearKvSession(
        self,
        mapKey: typing.Optional[str] = None,
        kV=None,
        uid: typing.Optional[str] = None,
    ):
        if not uid:
            uid = self.uploadId
        response = True
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        try:
            # remove expired entry and temp files
            res = kV.clearSessionKey(uid)
            # still must remove map table entry (key = file parameters)
            if not res:
                response = False
            if self.cP.get("KV_MODE") == "sqlite":
                kV.clearMapVal(uid)
            elif self.cP.get("KV_MODE") == "redis":
                if not mapKey:
                    response = False
                else:
                    kV.clearMap(mapKey)
        except Exception:
            return False
        return response

    # clear entire database
    async def clearKv(self, kV=None):
        if kV:
            pass
        elif self.cP.get("KV_MODE") == "sqlite":
            kV = KvSqlite(self.cP)
        elif self.cP.get("KV_MODE") == "redis":
            kV = KvRedis(self.cP)
        kV.clearTable(kV.sessionTable)
        kV.clearTable(kV.mapTable)

    # SESSION DIRECTORY FUNCTIONS

    def getPlaceholderFile(self, tempPath):
        repositoryType = os.path.basename(os.path.dirname(os.path.dirname(tempPath)))
        depId = os.path.basename(os.path.dirname(tempPath))
        uploadId = os.path.basename(tempPath)
        if uploadId.startswith("."):
            uploadId = uploadId[1:]
        if uploadId.startswith("_"):
            uploadId = uploadId[1:]
        sessionDirPath = self.cP.get("SESSION_DIR_PATH")
        placeholder = os.path.join(
            sessionDirPath,
            "%s~%s~%s" % (repositoryType, depId, uploadId),
        )
        return placeholder

    def makePlaceholderFile(self, tempPath):
        placeholder = self.getPlaceholderFile(tempPath)
        if not os.path.exists(os.path.dirname(placeholder)):
            os.makedirs(os.path.dirname(placeholder))
        if not os.path.exists(placeholder):
            with open(placeholder, "wb"):
                os.utime(placeholder, (time.time(), time.time()))

    def removePlaceholderFile(self, tempPath):
        placeholder = self.getPlaceholderFile(tempPath)
        if os.path.exists(placeholder):
            logging.info("removing placeholder file %s", placeholder)
            os.unlink(placeholder)
        else:
            logging.exception("error - placeholder file does not exist %s", placeholder)

    # LOCKING FUNCTIONS

    def getLockPath(self, filePath):
        # make lock path from either temp file path or target file path
        repositoryType = os.path.basename(os.path.dirname(os.path.dirname(filePath)))
        depId = os.path.basename(os.path.dirname(filePath))
        uploadId = os.path.basename(filePath)
        if uploadId.startswith("."):
            uploadId = uploadId[1:]
        if uploadId.startswith("_"):
            uploadId = uploadId[1:]
        sharedLockDirPath = self.cP.get("SHARED_LOCK_PATH")
        if not os.path.exists(sharedLockDirPath):
            os.makedirs(sharedLockDirPath)
        lockPath = os.path.join(
            sharedLockDirPath, repositoryType + "~" + depId + "~" + uploadId + ".lock"
        )
        return lockPath

    # unused locking functions

    def getLockPathFromParameters(
        self,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
    ) -> str:
        # legacy function
        sharedLockDirPath = self.cP.get("SHARED_LOCK_PATH")
        if not os.path.exists(sharedLockDirPath):
            os.makedirs(sharedLockDirPath)
        fnBase = PathProvider(self.cP).getBaseFileName(
            depId, contentType, milestone, partNumber, contentFormat
        )
        return os.path.join(sharedLockDirPath, fnBase + ".lock")

    def getInPlaceLockPath(self, filePath):
        # make lock path from target file path (not temp file path)
        lockPath = os.path.join(
            os.path.dirname(filePath), "." + os.path.basename(filePath) + ".lock"
        )
        return lockPath

    # BULK SESSION MAINTENANCE

    @staticmethod
    async def cleanupSessions(seconds=None):
        # by default removes only unexpired sessions
        # set max seconds <= 0 to remove all sessions
        # set to None to keep unexpired sessions
        cP = ConfigProvider()
        repositoryDir = cP.get("REPOSITORY_DIR_PATH")
        sessionDir = cP.get("SESSION_DIR_PATH")
        lockDir = cP.get("SHARED_LOCK_PATH")
        kvMaxSeconds = cP.get("KV_MAX_SECONDS")
        kvMode = cP.get("KV_MODE")
        if kvMode != "sqlite" and kvMode != "redis":
            logging.exception("error - unknown kv mode")
            return False
        if seconds is None:
            seconds = kvMaxSeconds
        for placeholder in os.listdir(sessionDir):
            placeholder_path = os.path.join(sessionDir, placeholder)
            if os.path.isfile(placeholder_path):
                try:
                    repoType, depId, sessionId = placeholder.split("~")
                except ValueError as exc:
                    logging.exception("error for path %s %r", placeholder_path, exc)
                    return
                modTime = os.path.getmtime(placeholder_path)
                elapsed = time.time() - float(modTime)
                if elapsed >= float(seconds):
                    logging.info("clearing %s", placeholder_path)
                    # clear kv
                    if kvMode == "sqlite":
                        kV = KvSqlite(cP)
                    elif kvMode == "redis":
                        kV = KvRedis(cP)
                    try:
                        # remove expired entry and temp files
                        if not kV.clearSessionKey(sessionId):
                            logging.exception(
                                "error - could not remove session key for %s", sessionId
                            )
                        # still must remove map table entry (key = file parameters)
                        if kvMode == "sqlite":
                            kV.clearMapVal(sessionId)
                        elif kvMode == "redis":
                            kV.deleteMapKeyFromVal(sessionId)
                    except Exception:
                        pass
                    # clear lock files
                    lockPath = os.path.join(lockDir, placeholder + ".lock")
                    if os.path.exists(lockPath):
                        os.unlink(lockPath)
                    # clear temp files
                    dirPath = os.path.join(repositoryDir, repoType, depId)
                    tempPath = Sessions(cP, sessionId).getTempFilePath(
                        dirPath, sessionId
                    )
                    if os.path.exists(tempPath):
                        os.unlink(tempPath)
                    # remove placeholder file
                    if os.path.exists(placeholder_path):
                        os.unlink(placeholder_path)


# invoke with arg 0 to remove all sessions
# invoke with no args to remove only unexpired sessions (as specified in config.yml kv_max_seconds)

if __name__ == "__main__":
    maxSeconds = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        maxSeconds = sys.argv[1]
    asyncio.run(Sessions.cleanupSessions(maxSeconds))
