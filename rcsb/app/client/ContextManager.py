from rcsb.app.client.ClientUtils import ClientUtils

class ContextManager(object):
    def __init__(self, repositoryType, depositId, contentType, partNumber, milestone, contentFormat, version):
        self.__cU = ClientUtils()
        self.__hashType = 'MD5'
        self.__repositoryType = repositoryType
        self.__depositId = depositId
        self.__contentType = contentType
        self.__partNumber = partNumber
        self.__milestone = milestone
        self.__contentFormat = contentFormat
        self.__version = version
        self.__file = self.__cU.download(repositoryType, depositId, contentType, partNumber, milestone, contentFormat, version, self.__hashType, None, False, True)
    def __enter__(self):
        return self.__file
    def __exit__(self, type, value, traceback):
        self.__cU.upload(self.__file.name, self.__repositoryType, self.__depositId, self.__contentType, self.__partNumber, self.__milestone, self.__contentFormat, self.__version, False, True, False)

if __name__ == '__main__':
    with ContextManager("deposit", "D_000", "model", "upload", 1, "pdbx", 1) as cm:
        cm.seek(0)
        bytes = cm.read(64)
        print(bytes.decode())
