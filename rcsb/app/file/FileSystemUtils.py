##
# File:    FileSystemUtils.py
# Author:  jdw
# Date:    25-Aug-2021
# Version: 0.001
#
# Updates:
##
"""
Collected utilities for file system path and file name access.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import glob
import logging
import os


logger = logging.getLogger(__name__)


class FileSystemUtils:
    """Collected utilities for file system path and file name access."""

    def __init__(self):
        pass

    def getRepositoryPath(self, repository):
        if repository.lower() in ["onedep-archive", "onedep-deposit"]:
            return os.environ.get("REPOSITORY_PATH", ".")
        return None

    def getVersionedPath(self, version, repoPath, idCode, contentType, partNumber, contentFormat):
        fTupL = []
        filePath = None
        try:
            fnBase = f"{idCode}_{contentType}_P{partNumber}.{contentFormat}.V"
            filePattern = os.path.join(repoPath, idCode, fnBase)
            for pth in glob.iglob(filePattern + "*"):
                vNo = int(pth.split(".")[-1][1:])
                fTupL.append((pth, vNo))
            # - sort in decending version order -
            fTupL.sort(key=lambda tup: tup[1], reverse=True)
            if version.lower() == "next":
                filePath = filePattern + str(fTupL[0][1] + 1)
            elif version.lower() == ["last", "latest"]:
                filePath = fTupL[0][0]
            elif version.lower() in ["prev", "previous"]:
                filePath = fTupL[1][0]
            elif version.lower() in ["first"]:
                filePath = fTupL[-1][0]
            elif version.lower() in ["second"]:
                filePath = fTupL[-2][0]

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return filePath

    def getMimeType(self, contentFormat):
        if contentFormat in ["cif"]:
            mt = "chemical/x-mmcif"
        elif contentFormat in ["pdf"]:
            mt = "application/pdf"
        elif contentFormat in ["xml"]:
            mt = "application/xml"
        elif contentFormat in ["json"]:
            mt = "application/json"
        elif contentFormat in ["txt"]:
            mt = "text/plain"
        elif contentFormat in ["pic"]:
            mt = "application/python-pickle"
        else:
            mt = "text/plain"

        return mt
