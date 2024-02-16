##
# File:    UploadUtility.py
# Author:  jdw
# Date:    30-Aug-2021
# Version: 0.001
#
# Updates: James Smith
#

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import gzip
import bz2
import io
import lzma
import shutil
import zipfile
import logging
import os
import typing
import aiofiles
from fastapi import HTTPException
from rcsb.app.file.Sessions import Sessions
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.IoUtility import IoUtility
from rcsb.app.file.serverStatus import ServerStatus


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


class UploadUtility(object):
    """
    functions - get upload parameters, upload, compress file, decompress file, compress chunk, decompress chunk
    """
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
        chunkSize: int,  # bytes
        chunkIndex: int,
        expectedChunks: int,
        # upload file parameters
        uploadId: str,
        hashType: str,
        hashDigest: str,
        # save file parameters
        filePath: str,
        fileSize: int,  # bytes
        fileExtension: str,
        decompress: bool,
        allowOverwrite: bool,
        # other
        resumable: bool,
        extractChunk: bool,
    ):
        df = ServerStatus.getServerStorage()["repository disk bytes free"]
        if chunkIndex == 0 and fileSize and isinstance(fileSize, int) and df:
            if fileSize >= df:
                raise HTTPException(status_code=507, detail="error - repository disk full")
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
        if chunkSize and isinstance(chunkSize, int):
            if chunkSize >= df:
                await session.close(tempPath, resumable, mapKey)
                raise HTTPException(status_code=507, detail="error - repository disk full")
        contents = chunk.read()
        # empty chunk beyond loop index from client side, don't erase temp file so keep out of try block
        if contents and len(contents) <= 0:
            # outside of try block an exception will exit
            chunk.close()
            raise HTTPException(status_code=400, detail="error - empty file")
        if extractChunk:
            compressionType = self.cP.get("COMPRESSION_TYPE")
            contents = await self.decompressChunk(contents, compressionType)
        try:
            # save, then compare hash or file size, then decompress
            # should lock, however client must wait for each response before sending next chunk, precluding race conditions (unless multifile upload problem)
            async with aiofiles.open(tempPath, "ab") as ofh:
                await ofh.write(contents)
            # if last chunk
            if chunkIndex + 1 == expectedChunks:
                # need not lock temp file
                if hashDigest and hashType:
                    if not IoUtility().checkHash(tempPath, hashDigest, hashType):
                        raise HTTPException(
                            status_code=400, detail=f"{hashType} hash comparison failed"
                        )
                elif fileSize:
                    if fileSize != os.path.getsize(tempPath):
                        raise HTTPException(
                            status_code=400,
                            detail="Error - file size comparison failed",
                        )
                else:
                    raise HTTPException(
                        status_code=400, detail="Error - no hash or file size provided"
                    )
                # last minute race condition handling
                if os.path.exists(filePath) and not allowOverwrite:
                    raise HTTPException(
                        status_code=403,
                        detail="Encountered existing file - cannot overwrite",
                    )
                # lock target file (though it might not exist) then save
                try:
                    async with Locking(filePath, "w"):
                        # save final version
                        os.replace(tempPath, filePath)
                        # decompress
                        if decompress and fileExtension:
                            await self.decompressFile(filePath, fileExtension)
                except (FileExistsError, OSError) as err:
                    raise HTTPException(
                        status_code=400, detail="error %r" % err
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

    async def decompressFile(self, inputFilePath: str, fileExtension: str) -> str:
        """

        Args:
            inputFilePath: path of file on server
            fileExtension: compression type

        Returns:
            new file path/name

        source - rcsb.utils.io.FileUtil
        author - John Westbrook
        (with modifications)

        """
        try:
            if not fileExtension.startswith("."):
                fileExtension = "." + fileExtension
            if fileExtension not in [".gz", ".bz2", ".xz", ".zip"]:
                logging.error("error - unknown file extension %s", fileExtension)
                return None
            decompressedFilePath = inputFilePath
            compressedFilePath = inputFilePath + fileExtension
            outputDir = os.path.basename(inputFilePath)
            # rename from deposition format to compressed file name
            # then decompress from compressed file name to original file name
            os.replace(inputFilePath, compressedFilePath)
            if compressedFilePath.endswith(".gz"):
                with gzip.open(compressedFilePath, mode="rb") as inpF:
                    with io.open(decompressedFilePath, "wb") as outF:
                        shutil.copyfileobj(inpF, outF)
            elif compressedFilePath.endswith(".bz2"):
                with bz2.open(compressedFilePath, mode="rb") as inpF:
                    with io.open(decompressedFilePath, "wb") as outF:
                        shutil.copyfileobj(inpF, outF)
            elif compressedFilePath.endswith(".xz"):
                with lzma.open(compressedFilePath, mode="rb") as inpF:
                    with io.open(decompressedFilePath, "wb") as outF:
                        shutil.copyfileobj(inpF, outF)
            elif compressedFilePath.endswith(".zip"):
                with zipfile.ZipFile(compressedFilePath, mode="r") as zObj:
                    memberList = zObj.namelist()
                    for member in memberList[:1]:
                        zObj.extract(member, path=outputDir)
                if memberList:
                    # return file name of first file in zip archive
                    outputFilePath = os.path.join(outputDir, memberList[0])
                    # rename file in zip archive from compressed file format to deposition format
                    os.replace(outputFilePath, inputFilePath)
            else:
                outputFilePath = inputFilePath
            # remove compressed file
            os.unlink(compressedFilePath)
            if os.path.exists(compressedFilePath):
                logging.warning("error - file still exists %s", compressedFilePath)
        except Exception as e:
            logging.exception(
                "Failing uncompress for file %s with %s", inputFilePath, str(e)
            )
        logging.debug("Returning file path %r", decompressedFilePath)
        return decompressedFilePath

    def compressFile(
        self, readFilePath: str, saveFilePath: str = None, compressionType: str = "gzip"
    ) -> str:
        """

        Args:
            readFilePath: client side path
            saveFilePath: server side path (zip only)
            compressionType: gzip, bzip2, zip, or lzma

        Returns:
            new file path with appropriate extension

        """
        if compressionType == "gzip":
            tempPath = readFilePath + ".gz"
            with open(readFilePath, "rb") as r:
                with gzip.open(tempPath, "wb") as w:
                    w.write(r.read())
            readFilePath = tempPath
        elif compressionType == "bzip2":
            tempPath = readFilePath + ".bz2"
            with open(readFilePath, "rb") as r:
                with bz2.open(tempPath, "wb") as w:
                    w.write(r.read())
            readFilePath = tempPath
        elif compressionType == "zip":
            tempPath = readFilePath + ".zip"
            # file name inside of zip archive
            # after extraction on server, file will have this name
            targetfilename = os.path.basename(saveFilePath)
            # create zip archive with one file inside
            with zipfile.ZipFile(tempPath, "w") as w:
                w.write(readFilePath, targetfilename)
            readFilePath = tempPath
        elif compressionType == "lzma":
            tempPath = readFilePath + ".xz"
            with open(readFilePath, "rb") as r:
                with lzma.open(tempPath, "wb") as w:
                    w.write(r.read())
            readFilePath = tempPath
        return readFilePath

    def compressChunk(self, chunk, compressionType):
        if compressionType == "gzip":
            return gzip.compress(chunk)
        elif compressionType == "bzip2":
            return bz2.compress(chunk)
        elif compressionType == "lzma":
            return lzma.compress(chunk)
        elif compressionType == "zip":
            logging.error("error - cannot compress chunks with zip file compression")
            return None
        else:
            logging.error("error - unknown compression type for file extension")
            return None

    async def decompressChunk(self, chunk, compressionType):
        if compressionType == "gzip":
            return gzip.decompress(chunk)
        elif compressionType == "bzip2":
            return bz2.decompress(chunk)
        elif compressionType == "lzma":
            return lzma.decompress(chunk)
        elif compressionType == "zip":
            raise HTTPException(
                status_code=400,
                detail="error - cannot extract chunks with zip file compression",
            )
        else:
            raise HTTPException(
                status_code=400, detail="error - unknown compression type"
            )
