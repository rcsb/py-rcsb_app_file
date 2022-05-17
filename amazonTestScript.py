import requests
import os
import logging

logger = logging.getLogger()

fileName = "./testFile.txt"

# create file for download
# select size of file here (in bytes)
nB = 1000000
with open(fileName, "wb") as ofh:
    ofh.write(os.urandom(nB))

with open(fileName, "rb") as ifh:
    files = {"uploadFile": ifh}
    r = requests.post("http://0.0.0.0:80/file-v1/upload-aws", files=files)

print(r.text)
print(r.status_code)
