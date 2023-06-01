##
# File: DownloadRequest.py
# Date: 11-Aug-2021
# Updates: James Smith 2023

##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

"""
    Download a single file
    Download/upload a session bundle
"""

import logging
import typing
from enum import Enum
from fastapi import APIRouter
from fastapi import Query
from rcsb.app.file.DownloadUtility import DownloadUtility

logger = logging.getLogger(__name__)

# not possible to secure an HTML form with a JWT, so no dependencies
router = APIRouter(tags=["download"])


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


@router.get("/download")
async def download(
    repositoryType: str = Query(
        None,
        title="Repository Type",
        description="Repository type (onedep-archive,onedep-deposit)",
        example="onedep-archive, onedep-deposit",
    ),
    depId: str = Query(
        None, title="ID Code", description="Identifier code", example="D_0000000001"
    ),
    contentType: str = Query(
        None,
        title="Content type",
        description="Content type",
        example="model, structure-factors, val-report-full",
    ),
    milestone: str = Query(
        "", title="milestone", description="milestone", example="release"
    ),
    partNumber: int = Query(
        1, title="Content part", description="Content part", example="1,2,3"
    ),
    contentFormat: str = Query(
        None,
        title="Content format",
        description="Content format",
        example="pdb, pdbx, mtz, pdf",
    ),
    version: str = Query(
        "1",
        title="Version string",
        description="Version number or description",
        example="1,2,3, latest, previous",
    ),
    hashType: HashType = Query(
        None, title="Hash type", description="Hash type", example="SHA256"
    ),
    chunkSize: typing.Optional[int] = None,
    chunkIndex: typing.Optional[int] = None,
):
    return await DownloadUtility().download(
        repositoryType,
        depId,
        contentType,
        milestone,
        partNumber,
        contentFormat,
        version,
        hashType,
        chunkSize,
        chunkIndex,
    )
