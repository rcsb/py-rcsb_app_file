import aioboto3
import asyncio
import os
from boto3.s3.transfer import TransferConfig


async def aioboto3upload(fileobject, bucket, key):
    session = aioboto3.Session()
    async with session.client('s3', region_name="us-east-1", aws_secret_access_key="", aws_access_key_id="") as client:

        config = TransferConfig(multipart_chunksize=8388608)

        # with open("./tempfile", "wb") as ofh:
        #     ofh.write(fileobject)

        # uploadFile = fileobject.file

        await client.upload_file("testFile.txt", bucket, key, Config=config)
        return "Done"


async def funcCall():
    filename = "testFile.txt"
    bucket = "rcsb-file-api"
    key = "newTestFile"
    response = await aioboto3upload(filename, bucket, key)
    print(response)

# nB = 5000000000
# with open("/mnt/vdb1/testFile.txt", "wb") as ofh:
#     ofh.write(os.urandom(nB))
# print("file written")

asyncio.run(funcCall())
# await funcCall()
