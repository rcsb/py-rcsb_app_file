##
# File:    UploadUtility.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates: James Smith, Ahsan Tanweer 2023
#

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import gzip
import logging
import os
import typing
import aiofiles
from filelock import Timeout, FileLock
from fastapi import HTTPException
from rcsb.app.file.Sessions import Sessions
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.IoUtility import IoUtility
from rcsb.utils.io.FileUtil import FileUtil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)

# functions - get upload parameters, upload


class UploadUtility(object):
    def __init__(self, cP: typing.Type[ConfigProvider] = None):
        self.cP = cP if cP else ConfigProvider()

    async def getUploadParameters(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: typing.Optional[str],
        partNumber: int,
        contentFormat: str,
        version: str,
        allowOverwrite: bool,
        resumable: bool,
    ):
        if not PathProvider(self.cP).validateParameters(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        ):
            raise HTTPException(status_code=400, detail="invalid parameters")
        # create session
        uploadId = None
        session = Sessions(uploadId=uploadId, cP=self.cP)
        await session.open(
            resumable,
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
        # get upload id
        uploadId = session.uploadId
        # get truncated target file path to return to client
        # path requires prefix of repository dir path
        try:
            resultPath = session.getSaveFilePath(
                repositoryType,
                depId,
                contentType,
                milestone,
                partNumber,
                contentFormat,
                version,
                allowOverwrite,
            )
        except FileExistsError:
            raise HTTPException(
                status_code=403,
                detail="Encountered existing file - overwrite prohibited",
            )
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Error - could not make file path from parameters",
            )
        # remove file name
        dirPath, _ = os.path.split(resultPath)
        # add absolute path and create dirs if necessary
        repositoryDirPath = self.cP.get("REPOSITORY_DIR_PATH")
        fullPath = os.path.join(repositoryDirPath, dirPath)
        defaultFilePermissions = self.cP.get("DEFAULT_FILE_PERMISSIONS")
        if not os.path.exists(fullPath):
            os.makedirs(fullPath, mode=defaultFilePermissions, exist_ok=True)
        # get chunk index
        uploadCount = 0
        if resumable:
            uploadCount = await session.getUploadCount(fullPath)
            if uploadCount > 0:
                logging.info("resuming upload on chunk %d", uploadCount)
        return {"filePath": resultPath, "chunkIndex": uploadCount, "uploadId": uploadId}

    # in-place sequential chunk
    async def upload(
        self,
        # chunk parameters
        chunk: typing.IO,
        chunkSize: int,
        chunkIndex: int,
        expectedChunks: int,
        # upload file parameters
        uploadId: str,
        hashType: str,
        hashDigest: str,
        # save file parameters
        filePath: str,
        fileSize: int,
        fileExtension: str,
        decompress: bool,
        allowOverwrite: bool,
        # other
        resumable: bool,
        extractChunk: bool,
    ):
        repositoryPath = self.cP.get("REPOSITORY_DIR_PATH")
        filePath = os.path.join(repositoryPath, filePath)
        session = Sessions(uploadId=uploadId, cP=self.cP)
        sessionKey = uploadId
        mapKey = None
        if resumable:
            repositoryType = os.path.basename(
                os.path.dirname(os.path.dirname(filePath))
            )
            mapKey = session.getKvPreparedMapKey(repositoryType, filePath)
            # on first chunk upload, set chunk size, record uid in map table
            if chunkIndex == 0:
                await session.setKvSession(sessionKey, "chunkSize", chunkSize)
                await session.setKvMap(mapKey, sessionKey)

        # logging.info("chunk %s of %s for %s", chunkIndex, expectedChunks, uploadId)

        dirPath, _ = os.path.split(filePath)
        tempPath = session.getTempFilePath(dirPath)
        if chunkIndex == 0:
            session.makePlaceholderFile(tempPath)
        contents = chunk.read()
        # empty chunk beyond loop index from client side, don't erase temp file so keep out of try block
        if contents and len(contents) <= 0:
            # outside of try block an exception will exit
            chunk.close()
            raise HTTPException(status_code=400, detail="error - empty file")
        if extractChunk:
            contents = gzip.decompress(contents)
        try:
            # save, then compare hash or file size, then decompress
            # should lock, however client must wait for each response before sending next chunk, precluding race conditions (unless multifile upload problem)
            async with aiofiles.open(tempPath, "ab") as ofh:
                await ofh.write(contents)
            # if last chunk
            if chunkIndex + 1 == expectedChunks:
                if hashDigest and hashType:
                    if not IoUtility().checkHash(tempPath, hashDigest, hashType):
                        raise HTTPException(
                            status_code=400, detail=f"{hashType} hash comparison failed"
                        )
                elif fileSize:
                    if fileSize != os.path.getsize(tempPath):
                        raise HTTPException(status_code=400, detail="Error - file size comparison failed")
                else:
                    raise HTTPException(status_code=400, detail="Error - no hash or file size provided")
                # lock then save
                lockPath = session.getLockPath(tempPath)  # either tempPath or filePath
                lock = FileLock(lockPath)
                try:
                    with lock.acquire(timeout=60 * 60 * 4):
                        # last minute race condition handling
                        if os.path.exists(filePath) and not allowOverwrite:
                            raise HTTPException(
                                status_code=403,
                                detail="Encountered existing file - cannot overwrite",
                            )
                        else:
                            # save final version
                            os.replace(tempPath, filePath)
                except Timeout:
                    raise HTTPException(
                        status_code=400,
                        detail=f"error - lock timed out on {filePath}",
                    )
                finally:
                    lock.release()
                    if os.path.exists(lockPath):
                        os.unlink(lockPath)
                # decompress
                if decompress and fileExtension:
                    if fileExtension.startswith("."):
                        fileExtension = fileExtension[1:]
                    if fileExtension.find(".") < 0:
                        # rename file with original file extension to enable decompression
                        compressedFilePath = "%s.%s" % (filePath, fileExtension)
                        os.replace(filePath, compressedFilePath)
                        FileUtil().uncompress(
                            compressedFilePath, os.path.dirname(filePath)
                        )
                        os.unlink(compressedFilePath)
                    else:
                        os.unlink(filePath)
                        raise HTTPException(
                            status_code=400,
                            detail="error - double file extension - could not decompress",
                        )
                # clear database and temp files
                await session.close(tempPath, resumable, mapKey)
        except HTTPException as exc:
            await session.close(tempPath, resumable, mapKey)
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        except Exception as exc:
            await session.close(tempPath, resumable, mapKey)
            raise HTTPException(
                status_code=400, detail=f"error in sequential upload {str(exc)}"
            )
        finally:
            chunk.close()
