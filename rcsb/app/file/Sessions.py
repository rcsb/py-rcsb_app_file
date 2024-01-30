# file: Sessions.py
# author: James Smith 2023

import asyncio
import json
import os
import sys
import time
import logging
import uuid
import typing
from rcsb.app.file.KvBase import KvBase
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.KvRedis import KvRedis
from rcsb.app.file.KvSqlite import KvSqlite

provider = ConfigProvider()
locktype = provider.get("LOCK_TYPE")
if locktype == "redis":
    from rcsb.app.file.RedisLock import Locking
elif locktype == "ternary":
    from rcsb.app.file.TernaryLock import Locking
else:
    from rcsb.app.file.SoftLock import Locking

logging.basicConfig(level=logging.INFO)


class Sessions(object):
    """
    session maintenance for one upload
    includes optional resumability API
    functions - upload helper functions, database functions, session placeholder functions, bulk maintenance function
    """
    def __init__(self, uploadId=None, cP=None, kV=True):
        """
        statelessly invoked once per chunk of every upload
        """
        self.uploadId = uploadId
        self.cP = cP if cP else ConfigProvider()
        self.kV = None
        if kV:
            self.kV = KvBase(self.cP)

    # invoked only once per upload
    async def open(
        self,
        resumable,
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
    ):
        if resumable:
            self.uploadId = await self.getResumedUpload(
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
            )
            if self.uploadId is not None:
                logging.info("resuming session %s", self.uploadId)
        if self.uploadId is None:
            self.uploadId = self.getNewUploadId()

    # UPLOAD HELPER FUNCTIONS

    def getNewUploadId(self):
        return uuid.uuid4().hex

    # find upload id using file parameters
    async def getResumedUpload(
        self,
        repositoryType: str = "archive",
        depId: str = None,
        contentType: str = "model",
        milestone: str = None,
        partNumber: int = 1,
        contentFormat: str = "pdbx",
        version: str = "next",
    ):  # returns uploadId or None
        mapKey = await self.getKvPrimaryMapKey(
            repositoryType=repositoryType,
            depId=depId,
            contentType=contentType,
            milestone=milestone,
            partNumber=partNumber,
            contentFormat=contentFormat,
            version=version,
        )
        uploadId = self.kV.getMap(mapKey)
        if uploadId is None:
            # not a resumed upload
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
    async def close(
        self,
        tempPath: str,
        resumable: bool = False,
        mapKey: typing.Optional[str] = None,
        uid: typing.Optional[str] = None,
    ):
        if os.path.exists(tempPath):
            os.unlink(tempPath)
        self.removePlaceholderFile(tempPath)
        if not resumable:
            return True
        if not uid:
            uid = self.uploadId
        return await self.clearKvSession(mapKey, uid)

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

    # returns entire dictionary of session table entry
    async def getKvSession(self, uploadId=None):
        if uploadId is None:
            uploadId = self.uploadId
        return self.kV.getKey(uploadId, self.kV.sessionTable)

    async def setKvSession(self, key1, key2, val):
        self.kV.setSession(key1, key2, val)

    async def setKvMap(self, key, val):
        self.kV.setMap(key, val)

    async def getKvPrimaryMapKey(
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

    # clear one entry from session table and corresponding entry from map table
    async def clearKvSession(
        self,
        mapKey: typing.Optional[str] = None,
        uid: typing.Optional[str] = None,
    ):
        if not uid:
            uid = self.uploadId
        response = True
        try:
            # remove expired sessions entry (key = upload id)
            res = self.kV.clearSessionKey(uid)
            # remove map table entry
            # key = file parameters, val = upload id
            # without file parameters, must find key from val
            if not res:
                response = False
            if mapKey is not None:
                self.kV.clearMapKey(mapKey)
            else:
                self.kV.clearMapVal(uid)
        except Exception:
            return False
        return response

    # SESSION DIRECTORY FUNCTIONS

    def getPlaceholderFile(self, tempPath):
        # placeholder name should overlap with lock file name so that cleanupSessions is able to remove associated lock files for a placeholder
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

    # BULK SESSION MAINTENANCE

    @staticmethod
    async def cleanupSessions(seconds=None):
        # triggered on server shutdown (remove all), cron job (remove expired), or from command line
        # by default removes only expired sessions
        # set max seconds <= 0 to remove all sessions
        # set to None to keep expired sessions
        cP = ConfigProvider()
        repositoryDir = cP.get("REPOSITORY_DIR_PATH")
        sessionDir = cP.get("SESSION_DIR_PATH")
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
                                "error - could not remove session key for %s, upload may not have been resumable",
                                sessionId,
                            )
                        # remove map table entry with val (key = file parameters, val = session id)
                        kV.clearMapVal(sessionId)
                    except Exception:
                        pass
                    # clear temp files
                    dirPath = os.path.join(repositoryDir, repoType, depId)
                    tempPath = Sessions(
                        uploadId=sessionId, cP=cP, kV=False
                    ).getTempFilePath(dirPath, sessionId)
                    if os.path.exists(tempPath):
                        os.unlink(tempPath)
                    # remove placeholder file
                    if os.path.exists(placeholder_path):
                        os.unlink(placeholder_path)
        # remove expired locks
        timeout = cP.get("LOCK_TIMEOUT")
        if not isinstance(timeout, int):
            timeout = 60
        await Locking.cleanup(True, timeout)


# invoke with arg 0 to remove all sessions
# invoke with no args to remove only unexpired sessions (as specified in config.yml kv_max_seconds)

if __name__ == "__main__":
    maxSeconds = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        maxSeconds = sys.argv[1]
    asyncio.run(Sessions.cleanupSessions(maxSeconds))
