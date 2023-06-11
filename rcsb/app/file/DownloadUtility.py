##
# File: DownloadUtility.py
# Date: 11-Aug-2021
# Updates: James Smith 2023

##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

"""
    Download a single file
    Download/upload a session bundle (not implemented)
"""

import logging
import os
import typing
from enum import Enum
from fastapi import HTTPException
from fastapi.responses import FileResponse, Response
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.Definitions import Definitions
from rcsb.app.file.IoUtility import IoUtility


logger = logging.getLogger(__name__)


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


# functions -
# download, get mime type


class DownloadUtility(object):
    def __init__(self):
        self.__fileFormatExtensionD = Definitions().getFileFormatExtD()

    async def download(
        self,
        repositoryType: str,
        depId: str,
        contentType: str,
        milestone: str,
        partNumber: int,
        contentFormat: str,
        version: str,
        hashType: HashType,
        chunkSize: typing.Optional[int],
        chunkIndex: typing.Optional[int],
    ):
        filePath = PathProvider().getVersionedPath(
            repositoryType,
            depId,
            contentType,
            milestone,
            partNumber,
            contentFormat,
            version,
        )
        if not filePath:
            raise HTTPException(
                status_code=421, detail="Bad or incomplete path metadata"
            )
        if not os.path.exists(filePath):
            raise HTTPException(
                status_code=404,
                detail="Request file path does not exist %s" % filePath,
            )
        if chunkSize is not None and chunkIndex is not None:
            # return only one chunk
            data = None
            try:
                with open(filePath, "rb") as r:
                    r.seek(chunkIndex * chunkSize)
                    data = r.read(chunkSize)
            except Exception:
                raise HTTPException(status_code=500, detail="error occurred while reading file")
            return Response(content=data, media_type="application/octet-stream")
        else:
            # return complete file
            tD = {}
            if hashType and not (chunkSize is not None and chunkIndex is not None):
                hashDigest = IoUtility().getHashDigest(filePath, hashType.name)
                tD = {"rcsb_hash_type": hashType.name, "rcsb_hexdigest": hashDigest}
            mimeType = self.getMimeType(contentFormat)
            return FileResponse(
                path=filePath,
                media_type=mimeType,
                filename=os.path.basename(filePath),
                headers=tD,
            )

    def getMimeType(self, contentFormat: str) -> str:
        cFormat = contentFormat
        if (
            self.__fileFormatExtensionD
            and (contentFormat in self.__fileFormatExtensionD)
            and self.__fileFormatExtensionD[contentFormat]
        ):
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
