##
# File: uploadRequest.py
# Date: 10-Aug-2021
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os.path
import shutil
from enum import Enum

import aiofiles
from fastapi import APIRouter
from fastapi import Depends
from fastapi import File
from fastapi import Form
from fastapi import UploadFile
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from rcsb.utils.io.CryptUtils import CryptUtils

logger = logging.getLogger(__name__)

router = APIRouter()


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class UploadResult(BaseModel):
    fileName: str = Field(None, title="Stored file name", description="Stored file name", example="D_00000000.cif.V3")
    success: bool = Field(None, title="Success status", description="Success status", example="True")


@router.post("/upload-shutil", response_model=UploadResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def uploadShutil(
    uploadFile: UploadFile = File(...),
    idCode: str = Form(None, title="ID Code", description="Identifier code", example="D_00000000"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
):
    ok = True
    fn = None
    try:
        # writeable path is injected here
        cachePath = os.environ.get("CACHE_PATH", ".")
        #
        logger.debug("idCode %r hash %r hashType %r", idCode, hashDigest, hashType)
        #
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("uploadFile %s (%r)", fn, ct)
        outPath = os.path.join(cachePath, fn)
        try:
            with open(outPath, "wb") as ofh:
                shutil.copyfileobj(uploadFile.file, ofh)
        except Exception as e:
            logger.error("Internal write error %r (%r) path %r: %s", fn, ct, outPath, str(e))
        finally:
            uploadFile.file.close()

        # Check hash
        if hash and hashType:
            hD = CryptUtils().getFileHash(outPath, hashType.name)
            ok = hashDigest == hD["hashDigest"]
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        ok = False
    return {"success": ok, "fileName": fn}


@router.post("/upload-aiof", response_model=UploadResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def uploadAiofiles(
    uploadFile: UploadFile = File(...),
    idCode: str = Form(None, title="ID Code", description="Identifier code", example="D_00000000"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
):
    ok = True
    fn = None
    try:
        # writeable path is injected here
        cachePath = os.environ.get("CACHE_PATH", ".")
        #
        logger.debug("idCode %r hash %r hashType %r", idCode, hashDigest, hashType)
        #
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("uploadFile %s (%r)", fn, ct)
        outPath = os.path.join(cachePath, fn)
        try:
            async with aiofiles.open(outPath, "wb") as ofh:
                await ofh.write(uploadFile.file.read())
        except Exception as e:
            logger.error("Internal write error %r (%r) path %r: %s", fn, ct, outPath, str(e))
        finally:
            uploadFile.file.close()
        #
        # Check hash
        if hash and hashType:
            hD = CryptUtils().getFileHash(outPath, hashType.name)
            ok = hashDigest == hD["hashDigest"]
    except Exception as e:
        logger.exception("Failing with %s", str(e))
        ok = False
    return {"success": ok, "fileName": fn}
