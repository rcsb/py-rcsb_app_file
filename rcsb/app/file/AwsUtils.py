import logging
import typing
import aioboto3
from boto3.s3.transfer import TransferConfig

from rcsb.app.file.ConfigProvider import ConfigProvider

logger = logging.getLogger(__name__)


class AwsUtils:

    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.__cP = cP
        self.awsKey = cP.get("ACCESS_KEY")
        self.awsSecret = cP.get("SECRET_KEY")
        self.region = cP.get("AWS_REGION")
        self.bucket = cP.get("AWS_BUCKET")

    async def upload(self, filename, key):
        """Asynchronous upload with aioboto3. Defaults to multipart if file size exceeds threshold set by TransferConfig."""
        session = aioboto3.Session()
        async with session.client("s3", region_name=self.region, aws_secret_access_key=self.awsSecret, aws_access_key_id=self.awsKey) as client:
            config = TransferConfig()
            try:
                await client.upload_file(Filename=filename, Bucket=self.bucket, Key=key, Config=config)
                ret = {"fileName": filename, "success": True, "statusCode": 200, "statusMessage": "Upload completed"}
            except Exception as e:
                ret = {"fileName": filename, "success": False, "statusCode": 400, "statusMessage": "Upload fails with %s" % str(e)}
        return ret

    async def download(self, key):
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=self.region,
            aws_secret_access_key=self.awsSecret,
            aws_access_key_id=self.awsKey
        ) as client:
            response = await client.get_object(Bucket=self.bucket, Key=key)
            content = (await response['Body'].read())
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info("File downloaded path : https://%s.s3.%s.amazonaws.com/%s", self.bucket, self.region, key)
                return content
        return False
