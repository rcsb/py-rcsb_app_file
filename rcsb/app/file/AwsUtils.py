import logging
import typing
import aioboto3
from botocore.errorfactory import ClientError
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
        """Asynchronous upload with aioboto3. Defaults to multipart if file size exceeds threshold set by TransferConfig.
        Args:
            filename (str): name or path of file to be uploaded
            key (str): name that file will be given when uploaded to s3 bucket
        Returns:
            (dict): {"success": True|False, "statusMessage": <text>}
        """
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
        """Asynchronous download with aioboto3.
        Args:
            key (str): name of file to be retrieved from s3 bucket
        Returns:
            (dict): {"success": True|False, "statusMessage": <text>}
        """
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=self.region,
            aws_secret_access_key=self.awsSecret,
            aws_access_key_id=self.awsKey
        ) as client:
            try:
                response = await client.get_object(Bucket=self.bucket, Key=key)
                content = (await response['Body'].read())

                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    logger.info("File downloaded path: https://%s.s3.%s.amazonaws.com/%s", self.bucket, self.region, key)
                    return content
            except Exception as e:
                logger.info("File download failed with %s", e)
                return False

    async def checkExists(self, key):
        """Checks if file exists in bucket using key.
        Args:
            key (str): name of file to be searched for in s3 bucket
        Returns:
            (bool): True|False
        """
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=self.region,
            aws_secret_access_key=self.awsSecret,
            aws_access_key_id=self.awsKey
        ) as client:
            try:
                await client.head_object(Bucket=self.bucket, Key=key)
            except ClientError as e:
                if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                    return False
            return True
