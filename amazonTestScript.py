import requests
import os
import logging
import time
from aiobotocore.session import get_session
import boto3

logger = logging.getLogger()

fileName = "./testFile.txt"


# # Upload Testing
# # create file for download
# # select size of file here (in bytes)
# nB = 100
# with open(fileName, "wb") as ofh:
#     ofh.write(os.urandom(nB))

# mD = {
#     "idCode": "D_00000001",
#     "repositoryType": "onedep-archive",
#     "contentType": "model",
#     "contentFormat": "pdbx",
#     "partNumber": 1,
#     "version": 1
# }

# with open(fileName, "rb") as ifh:
#     files = {"uploadFile": ifh}
#     r = requests.post("http://0.0.0.0:80/file-v1/upload-aws", files=files, data=mD)

# print(r.text)
# print(r.status_code)


# # Download Testing
# downloadDict = {"key": "archive/D_00000001/D_00000001_model_P1.cif.V1",
#                 "idCode": "D_00000001",
#                 "repositoryType": "onedep-archive",
#                 "contentType": "model",
#                 "contentFormat": "pdbx",
#                 "partNumber": 1,
#                 "version": 1
# }

# timeList = []
# numTimes = 1

# for i in range(0, numTimes):
#     start = time.time()
#     r = requests.get("http://0.0.0.0:80/file-v1/download-aws", params=downloadDict)

#     with open("./rcsb/app/tests-file/test-data/testFile" + str(i) + ".dat", "wb") as ifh:
#         ifh.write(r.content)
#     timeSpent = time.time() - start
#     timeList.append(timeSpent)

# avgTime = sum(timeList) / len(timeList)

# print("Average time for download:", avgTime)

bucket = "rcsb-file-api"
key = "testFile"

AWS_ACCESS_KEY_ID = ""  # os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = ""  # os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"  # os.environ.get("AWS_REGION")


def endMultipart():
    client = boto3.client('s3', region_name=AWS_REGION, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, aws_access_key_id=AWS_ACCESS_KEY_ID)

    uploadIdList = ["8jId2VLJ3St1CRSMOo__tnWEpto0H.w98PnwQQS5EsFqp1O67xvVZj9Bl89g3UBYLv3DW1dYDW0UTS3kQZXnvwqqfOzOkFpVd2sjZ_ebZtecDmfatTAEOdwLqZ7fnVhk",
    "JR8HIFC3P73WnwhX9OWcTRfDKllzAgT1jpvS3TYb6.yGs5Ej1.JZIVlz0fE7vbwiuP_IjHFlYw1bMHEeG2r_dYpdmG.QoeXQ.eDyZa6XXlAmWpAtecIYF5_Zb5Dg326_",
    "baHnvVlSqLf2LiYHIl62yXURr7IVsbjd3wMSAITw8FAywuoScytMy.gy_.KT0HKeKXbi7fquiRxZ._I_RLn_6Hv.hw1VvRwplmdOLuu_eK80tVgBOYVAvDxOo5oPS83e",
    "6cbk9.5sJ7ha8xezrkLhUSJrOMEyF5WiEf7zOH4bDxpcRiLXiq1hXVmIySpzSWN0vTzeDZLhF4Sfbw.sit8u64l7HgGnANabDJt3.QUBkDuAKSBQZsiXe9LSTyXUySK.",]

    for uploadId in uploadIdList:
        print(uploadId)
        client.abort_multipart_upload(
            Bucket=bucket,
            Key="newTestFile",
            UploadId=uploadId
        )


endMultipart()

# multiD = {
#     "idCode": "D_00000002",
#     "repositoryType": "onedep-archive",
#     "contentType": "model",
#     "contentFormat": "pdbx",
#     "partNumber": 1,
#     "version": 1
# }

# nB = 500000000
# with open(fileName, "wb") as ofh:
#     ofh.write(os.urandom(nB))

# with open(fileName, "rb") as f:
#     files = {"uploadFile": f}
#     r = requests.post("http://0.0.0.0:80/file-v1/upload-Multipart-aws", files=files, data=multiD)

# multiD = {
#     "idCode": "D_00000003",
#     "repositoryType": "onedep-archive",
#     "contentType": "model",
#     "contentFormat": "pdbx",
#     "partNumber": 1,
#     "version": 1
# }

# nB = 150000000
# with open(fileName, "wb") as ofh:
#     ofh.write(os.urandom(nB))

# with open(fileName, "rb") as f:
#     files = {"uploadFile": f}
#     r = requests.post("http://0.0.0.0:80/file-v1/upload-boto3", files=files, data=multiD)
#     print(r.status_code)

# multiD = {
#     "idCode": "D_00000004",
#     "repositoryType": "onedep-archive",
#     "contentType": "model",
#     "contentFormat": "pdbx",
#     "partNumber": 1,
#     "version": 1
# }

# nB = 5000000000
# with open(fileName, "wb") as ofh:
#     ofh.write(os.urandom(nB))
# print("file written")

# with open(fileName, "rb") as f:
#     files = {"uploadFile": f}
#     r = requests.post("http://0.0.0.0:80/file-v1/upload-aioboto3", files=files, data=multiD)
#     print(r.status_code)
