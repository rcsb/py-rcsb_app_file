##
# File: uploadRequest.py
# Date: 10-Aug-2021
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import gzip
import logging
import os
from enum import Enum
import requests
import datetime
from aiobotocore.session import get_session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import UploadFile
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from pydantic import Field
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.IoUtils import IoUtils
from rcsb.app.file.JWTAuthBearer import JWTAuthBearer
from mmcif.io.PdbxReader import PdbxReader
from mmcif.io.PdbxWriter import PdbxWriter

logger = logging.getLogger(__name__)

router = APIRouter()


class HashType(str, Enum):
    MD5 = "MD5"
    SHA1 = "SHA1"
    SHA256 = "SHA256"


class UploadResult(BaseModel):
    fileName: str = Field(None, title="Stored file name", description="Stored file name", example="D_0000000001_model_P1.cif.V3")
    success: bool = Field(None, title="Success status", description="Success status", example="True")
    statusCode: int = Field(None, title="HTTP status code", description="HTTP status code", example="200")
    statusMessage: str = Field(None, title="Status message", description="Status message", example="Success")


class UploadSliceResult(BaseModel):
    sliceCount: str = Field(None, title="Slice count", description="Slice count", example="2")
    success: bool = Field(None, title="Success status", description="Success status", example="True")


@router.post("/upload", response_model=UploadResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def upload(
    uploadFile: UploadFile = File(...),
    idCode: str = Form(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Form(None, title="Repository Type", description="OneDep repository type", example="deposit, archive"),
    contentType: str = Form(None, title="Content Type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    partNumber: int = Form(None, title="Part Number", description="OneDep part number", example="1"),
    contentFormat: str = Form(None, title="Content format", description="Content format", example="pdb, pdbx, mtz, pdf"),
    version: str = Form(None, title="Version", description="OneDep version number of descriptor", example="1, 2, latest, next"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
    copyMode: str = Form("native", title="Copy mode", description="Copy mode", example="shell|native|gzip_decompress"),
    allowOverWrite: bool = Form(False, title="Allow overwrite of existing files", description="Allow overwrite of existing files", example="False"),
):
    fn = None
    ct = None
    try:
        cachePath = os.environ.get("CACHE_PATH", ".")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        #
        logger.debug("idCode %r hash %r hashType %r", idCode, hashDigest, hashType)
        #
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("uploadFile %s (%r)", fn, ct)
        #
        if fn.endswith(".gz") or ct == "application/gzip":
            copyMode = "gzip_decompress"
        #
        logger.debug("hashType.name %r hashDigest %r", hashType, hashDigest)
        ioU = IoUtils(cP)
        ret = await ioU.storeUpload(
            uploadFile.file,
            repositoryType,
            idCode,
            contentType,
            partNumber,
            contentFormat,
            version,
            allowOverWrite=allowOverWrite,
            copyMode=copyMode,
            hashType=hashType,
            hashDigest=hashDigest,
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {"success": False, "statusCode": 400, "statusMessage": "Upload fails with %s" % str(e)}
    #
    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret


@router.post("/upload-slice", response_model=UploadSliceResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def uploadSlice(
    uploadFile: UploadFile = File(...),
    sliceIndex: int = Form(1, title="Index of the current chunk", description="Index of the current chunk", example="1"),
    sliceTotal: int = Form(1, title="Total number of chunks in the session", description="Total number of chunks in the session", example="5"),
    sessionId: str = Form(None, title="Session identifier", description="Unique identifier for the current session", example="9fe2c4e93f654fdbb24c02b15259716c"),
    copyMode: str = Form("native", title="Copy mode", description="Copy mode", example="shell|native|gzip_decompress"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
):
    ct = None
    ret = {}
    try:
        cachePath = os.environ.get("CACHE_PATH", ".")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        #
        fn = uploadFile.filename
        ct = uploadFile.content_type
        logger.debug("sliceIndex %d sliceTotal %d fn %r", sliceIndex, sliceTotal, fn)
        ioU = IoUtils(cP)
        ret = await ioU.storeSlice(uploadFile.file, sliceIndex, sliceTotal, sessionId, copyMode=copyMode, hashType=hashType, hashDigest=hashDigest)
        logger.debug("sliceIndex %d sliceTotal %d return %r", sliceIndex, sliceTotal, ret)
    except Exception as e:
        logger.exception("Failing for %r %r with %s", fn, ct, str(e))
        ret = {"success": False, "statusCode": 400, "statusMessage": "Slice upload fails with %s" % str(e)}

    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret


@router.post("/join-slice", response_model=UploadResult, dependencies=[Depends(JWTAuthBearer())], tags=["upload"])
async def joinUploadSlice(
    idCode: str = Form(None, title="ID Code", description="Identifier code", example="D_0000000001"),
    repositoryType: str = Form(None, title="Repository Type", description="OneDep repository type", example="deposit, archive"),
    contentType: str = Form(None, title="Content Type", description="OneDep content type", example="model, structure-factors, val-report-full"),
    partNumber: int = Form(None, title="Part Number", description="OneDep part number", example="1"),
    contentFormat: str = Form(None, title="Content format", description="Content format", example="pdb, pdbx, mtz, pdf"),
    version: str = Form(None, title="Version", description="OneDep version number of descriptor", example="1, 2, latest, next"),
    sliceTotal: int = Form(1, title="Total number of chunks in the session", description="Total number of chunks in the session", example="5"),
    sessionId: str = Form(None, title="Session identifier", description="Unique identifier for the current session", example="9fe2c4e93f654fdbb24c02b15259716c"),
    copyMode: str = Form("native", title="Copy mode", description="Copy mode", example="shell|native|gzip_decompress"),
    allowOverWrite: bool = Form(False, title="Allow overwrite of existing files", description="Allow overwrite of existing files", example="False"),
    hashDigest: str = Form(None, title="Hash digest", description="Hash digest", example="'0394a2ede332c9a13eb82e9b24631604c31df978b4e2f0fbd2c549944f9d79a5'"),
    hashType: HashType = Form(None, title="Hash type", description="Hash type", example="SHA256"),
):
    ret = {}
    try:
        cachePath = os.environ.get("CACHE_PATH", ".")
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(cachePath, configFilePath)
        #
        logger.debug("sliceTotal %d", sliceTotal)
        # ---
        ioU = IoUtils(cP)
        ret = await ioU.finalizeMultiSliceUpload(
            sliceTotal,
            sessionId,
            repositoryType,
            idCode,
            contentType,
            partNumber,
            contentFormat,
            version,
            allowOverWrite,
            copyMode,
            hashType=hashType,
            hashDigest=hashDigest,
        )
    except Exception as e:
        logger.exception("Failing for %r %r with %s", idCode, sliceTotal, str(e))
        ret = {"success": False, "statusCode": 400, "statusMessage": "Slice upload fails with %s" % str(e)}

    if not ret["success"]:
        raise HTTPException(status_code=405, detail=ret["statusMessage"])
    #
    return ret


@router.post("/merge")
async def merge(
        siftsFile: UploadFile = File(...),
        pdbID: str = Form(None)
):
    cachePath = "./rcsb/app/tests-file/test-data/mmcif/"
    pdbIDHash = pdbID[1:3]

    cifUrl = "https://ftp.wwpdb.org/pub/pdb/data/structures/divided/mmCIF/" + pdbIDHash + "/" + pdbID + ".cif.gz"
    cifPath = cachePath + pdbID + ".cif"
    cifTempPath = cachePath + pdbID + "_temp.cif.gz"

    ofh = open(cifTempPath, "wb")
    response = requests.get(cifUrl)
    ofh.write(response.content)
    ofh.close()

    ifh = gzip.open(cifTempPath, "rb")
    unzipMMCIF = ifh.read()
    ofh = open(cifPath, "wb")
    ofh.write(unzipMMCIF)
    ofh.close()

    siftsList = []
    siftFile = await siftsFile.read()
    siftsRead = PdbxReader(siftFile)
    siftsRead.read(siftsList)

    cifList = []
    with open("./mmcifData/" + pdbID + ".cif", "r", encoding="utf8") as ifh:
        cifRead = PdbxReader(ifh)
        cifRead.read(cifList)

    siftsCatNames = siftsList[0].getObjNameList()

    siftsAtomSite = siftsList[0].getObj("atom_site")
    siftsCatNames.pop(0)

    siftsAttributes = siftsAtomSite.getAttributeList()

    for i in siftsAttributes:
        cifList[0].getObj("atom_site").appendAttributeExtendRows(i)

    for i in siftsList[0].getObj("atom_site").getAttributeList():
        j = 0
        while j < len(siftsList[0].getObj("atom_site").getAttributeValueList(i)):
            cifList[0].getObj("atom_site").setValue(siftsList[0].getObj("atom_site").getAttributeValueList(i)[j], i, j)
            # print(i, ":", siftsList[0].getObj("atom_site").getAttributeValueList(i)[j])
            j += 1

    # for i in siftsCatNames:
    #     tempObj = siftsList[0].getObj(i)
    #     cifList[0].append(tempObj)

    with open("./mmcifData/" + pdbID + "_merged.cif", "w", encoding="utf8") as ofh:
        pdbxW = PdbxWriter(ofh)
        pdbxW.write(cifList)


@router.post("/upload-aws", status_code=200, description="***** Upload png asset to S3 *****")
async def send_request(
    uploadFile: UploadFile = File(...)
):
    AWS_ACCESS_KEY_ID = ""  # os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = ""  # os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = "us-east-1"  # os.environ.get("AWS_REGION")
    S3_Bucket = "rcsb-file-api"  # os.environ.get("S3_Bucket")
    S3_Key = "dockertest"  # os.environ.get("S3_Key")

    # s3_client = boto3.client('s3')
    s3_client = S3_SERVICE(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)

    filename = 'testFile9.dat'
    current_time = datetime.datetime.now()
    split_file_name = os.path.splitext(filename)   # split the file name into two different path (string + extention)
    file_name_unique = str(current_time.timestamp()).replace('.', '')  # for realtime application you must have genertae unique name for the file
    file_extension = split_file_name[1]  # file extention
    file = await uploadFile.read()
    with open(filename, 'wb') as f:
        f.write(file)
    uploads3 = await s3_client.upload_fileobj(bucket=S3_Bucket, key=S3_Key + file_name_unique + file_extension, fileobject=file)
    if uploads3:
        s3_url = f"https://{S3_Bucket}.s3.{AWS_REGION}.amazonaws.com/{S3_Key}{file_name_unique +  file_extension}"
        return {"status": "success", "image_url": s3_url}  # response added
    else:
        raise HTTPException(status_code=400, detail="Failed to upload in S3")


class S3_SERVICE(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region

    async def upload_fileobj(self, fileobject, bucket, key):
        session = get_session()
        async with session.create_client('s3', region_name=self.region,
                                         aws_secret_access_key=self.aws_secret_access_key,
                                         aws_access_key_id=self.aws_access_key_id) as client:
            file_upload_response = await client.put_object(Bucket=bucket, Key=key, Body=fileobject)
            if file_upload_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info("File uploaded path : https://%s.s3.%s.amazonaws.com/%s", bucket, self.region, key)
                return True
        return False
