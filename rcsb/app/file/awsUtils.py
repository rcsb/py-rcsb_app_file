import logging
import datetime
import math
import os
import typing
import boto3
import aioboto3
from boto3.s3.transfer import TransferConfig
from aiobotocore.session import get_session
from filechunkio import FileChunkIO

from rcsb.app.file.ConfigProvider import ConfigProvider

logger = logging.getLogger(__name__)


class awsUtils:

    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.__cP = cP
        self.aws_key = cP.get("ACCESS_KEY")
        self.aws_secret = cP.get("SECRET_KEY")
        self.region = cP.get("AWS_REGION")
        self.bucket = cP.get("AWS_BUCKET")

    async def upload_fileobj(self, fileobject, key):
        session = get_session()
        async with session.create_client('s3', region_name=self.region,
                                         aws_secret_access_key=self.aws_secret,
                                         aws_access_key_id=self.aws_key) as client:
            file_upload_response = await client.put_object(Bucket=self.bucket, Key=key, Body=fileobject)
            if file_upload_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info("File uploaded path : https://%s.s3.%s.amazonaws.com/%s", self.bucket, self.region, key)
                return True
        return False

    async def download_fileobj(self, bucket, key):
        session = get_session()
        async with session.create_client('s3', region_name=self.region,
                                         aws_secret_access_key=self.aws_secret,
                                         aws_access_key_id=self.aws_key) as client:
            file_download_response = await client.get_object(Bucket=bucket, Key=key)
            content = (await file_download_response['Body'].read())
            if file_download_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info("File downloaded path : https://%s.s3.%s.amazonaws.com/%s", bucket, self.region, key)
                return content
        return False

    async def upload_multipart(self, fileobject, bucket, key):
        session = get_session()
        async with session.create_client('s3', region_name=self.region, aws_secret_access_key=self.aws_secret, aws_access_key_id=self.aws_key) as client:
            multiPartResponse = await client.create_multipart_upload(Bucket=bucket, Key=key, Expires=datetime.datetime.utcnow() + datetime.timedelta(minutes=5))
            uploadID = multiPartResponse['UploadId']
            print(uploadID)
            part_number = 1

            part_info = {
                'Parts': []
            }

            with open("./tempfile", "wb") as ofh:
                ofh.write(await fileobject.read())


            # await fileobject.seek(0)

            source_size = os.stat("./tempfile").st_size

            chunk_size = 50000000
            chunk_count = int(math.ceil(source_size / float(chunk_size)))

            for i in range(chunk_count):
                offset = chunk_size * i
                fileBytes = min(chunk_size, source_size - offset)
                with FileChunkIO("./tempfile", 'r', offset=offset, bytes=fileBytes) as fp:

                    response = await client.upload_part(
                        Bucket=bucket,
                        Body=fp,
                        UploadId=uploadID,
                        PartNumber=part_number,
                        Key=key
                    )
                    print(response)
                part_info['Parts'].append(
                    {
                        'PartNumber': part_number,
                        'ETag': response['ETag']
                    }
                )
                part_number += 1

            print(part_info)

            response = await client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=uploadID,
                MultipartUpload=part_info
            )
            print(response)

    async def aioboto3upload(self, fileobject, bucket, key):
        session = aioboto3.Session()
        async with session.client('s3', region_name=self.region, aws_secret_access_key=self.aws_secret, aws_access_key_id=self.aws_key) as client:

            config = TransferConfig(multipart_chunksize=8388608)

            # with open("./tempfile", "wb") as ofh:
            #     ofh.write(fileobject)

            uploadFile = fileobject.file

            await client.upload_file("testFile.txt", bucket, key, Config=config)

    def boto3multipart(self, fileobject, bucket, key):
        client = boto3.resource('s3', region_name=self.region, aws_secret_access_key=self.aws_secret, aws_access_key_id=self.aws_key)

        MB = 1024 ** 2
        config = TransferConfig(multipart_threshold=5 * MB)

        with open("./tempfile", "wb") as ofh:
            ofh.write(fileobject.file.read())

        client.meta.client.upload_file("./tempfile", bucket, key, Config=config)
