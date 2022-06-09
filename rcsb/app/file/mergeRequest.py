import logging
import os
from pydantic import Field
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from fastapi import Form
from fastapi import Depends
from fastapi import APIRouter
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.app.file.ConfigProvider import ConfigProvider
from mmcif.io.PdbxWriter import PdbxWriter
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(JWTAuthBearer())], tags=["merge"])


class MergeResult(BaseModel):
    fileName: str = Field(None, title="Merged file name", description="Stored file name", example="1yy9_merged.cif")
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


@router.post("/merge", response_model=MergeResult, tags=["merge"])
async def merge(
        siftsPath: str = Form(None),
        pdbID: str = Form(None)
):
    """Merge SIFTS data with CIF file of provided PDB ID (must exist in PDBX_REPOSITORY path).

    Args:
        siftsPath (str): Path of file containing SIFTS data
        pdbID (str): pdbID of file to merge SIFTS data into
    Returns:
        (dict): {"success": True|False, "statusMessage": <text>}
    """
    cachePath = os.environ.get("CACHE_PATH")
    configFilePath = os.environ.get("CONFIG_FILE")
    cP = ConfigProvider(cachePath, configFilePath)

    ret = {}
    try:
        cachePath = cP.get("PDBX_REPOSITORY")

        fU = FileUtil(workPath=cachePath)
        if not fU.exists(cachePath):
            fU.mkdir(cachePath)

        mU = MarshalUtil(workDir=cachePath)

        logger.info("Merging PDBx file %r with sifts file %r", os.path.join(cachePath, pdbID + ".cif.gz"), siftsPath)

        cifList = mU.doImport(os.path.join(cachePath, pdbID + ".cif.gz"), fmt="mmcif")
        siftsList = mU.doImport(siftsPath, fmt="mmcif")

        siftsCatNames = siftsList[0].getObjNameList()

        siftsAtomSite = siftsList[0].getObj("atom_site")
        siftsCatNames = [c for c in siftsCatNames if c != "atom_site"]

        siftsAttributes = siftsAtomSite.getAttributeList()

        for i in siftsAttributes:
            cifList[0].getObj("atom_site").appendAttributeExtendRows(i)

        for i in siftsList[0].getObj("atom_site").getAttributeList():
            j = 0
            attrValList = siftsList[0].getObj("atom_site").getAttributeValueList(i)
            while j < len(attrValList):
                cifList[0].getObj("atom_site").setValue(attrValList[j], i, j)
                j += 1

        for i in siftsCatNames:
            tempObj = siftsList[0].getObj(i)
            cifList[0].append(tempObj)

        with open(cachePath + pdbID + "_merged.cif", "w", encoding="utf-8") as ofh:
            pdbxW = PdbxWriter(ofh)
            pdbxW.write(cifList)

        ret = {"success": True, "statusCode": 200, "statusMessage": "Merge Successful"}

    except Exception as e:
        logger.exception("Failing for %s, and %s", siftsPath, pdbID)
        ret = {"success": False, "statusCode": 400, "statusMessage": "mmCIF and SIFTS data merge fails with: %s" % str(e)}

    return ret
