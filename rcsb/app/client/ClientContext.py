from rcsb.app.client.ClientUtils import ClientUtils

"""
example
with ClientContext(repoType, depId, contentType, milestone, partNumber, contentFormat, version, hashType, returnContext, unit_test) as cc:
    cc.seek(0)  # required before reading
    bytes = cc.read(64)
    print(bytes)
    cc.seek(0)
    cc.write(os.urandom(1024))
"""

class ClientContext(object):
    def __init__(self, repositoryType, depositId, contentType, milestone, partNumber, contentFormat, version, hashType='MD5', returnContext=False, unit_test=False):
        self.repositoryType = repositoryType
        self.depositId = depositId
        self.contentType = contentType
        self.milestone = milestone
        self.partNumber = partNumber
        self.contentFormat = contentFormat
        self.version = version
        self.hashType = hashType
        self.returnContext = returnContext
        # download repository file
        # returns a local named temporary file
        downloadFolder = None
        allowOverwrite = True
        returnTempFile = self.returnContext
        self.file = None
        self.tempFilePath = None
        if returnTempFile:
            self.cU = ClientUtils(unit_test)
            self.file = self.cU.download(repositoryType, depositId, contentType, milestone, partNumber, contentFormat, version, self.hashType, downloadFolder, allowOverwrite, returnTempFile)
            self.tempFilePath = self.file.name

    def __enter__(self):
        if self.returnContext:
            return self.file

    def __exit__(self, type, value, traceback):
        if self.returnContext:
            decompress = False
            allowOverwrite = True
            resumable = False
            # update repository file
            self.cU.upload(self.tempFilePath, self.repositoryType, self.depositId, self.contentType, self.milestone, self.partNumber, self.contentFormat, self.version, decompress, allowOverwrite, resumable)
            # delete local file
            self.file.close()


