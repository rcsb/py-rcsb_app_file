import shutil
import unittest
import os
import hashlib
import logging
import rcsb.app.config.setConfig  # noqa: F401
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.client.python.ClientUtils import ClientUtils
from rcsb.utils.io.LogUtil import StructFormatter

# pylint: disable=wrong-import-position
# This environment must be set before main.app is imported
# HERE = os.path.abspath(os.path.dirname(__file__))
# TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
root_handler = logger.handlers[0]
root_handler.setFormatter(StructFormatter(fmt=None, mask=None))
logger.setLevel(logging.INFO)


class TestClientContext(unittest.TestCase):

    def setUp(self):
        self.__cU = ClientUtils(unit_test=True)
        self.__configFilePath = os.environ.get("CONFIG_FILE")
        self.__cP = ConfigProvider(self.__configFilePath)
        self.__chunkSize = self.__cP.get("CHUNK_SIZE")
        self.__hashType = self.__cP.get("HASH_TYPE")
        self.__dataPath = self.__cP.get("REPOSITORY_DIR_PATH")
        self.__repoType = "unit-test"
        self.__unitTestFolder = os.path.join(self.__dataPath, self.__repoType)
        self.__depId = "D_000"
        self.__contentType = "model"
        self.__milestone = "upload"
        if not self.__milestone or self.__milestone.strip() == "":
            self.__convertedMilestone = ""
        else:
            self.__convertedMilestone = f"-{self.__milestone}"
        self.__partNumber = 1
        self.__contentFormat = "pdbx"
        self.__convertedContentFormat = "cif"
        self.__version = 1
        self.__filePath = os.path.join(self.__unitTestFolder, self.__depId, f'{self.__depId}_{self.__contentType}{self.__convertedMilestone}_P{self.__partNumber}.{self.__convertedContentFormat}.V{self.__version}')
        if not os.path.exists(os.path.dirname(self.__filePath)):
            os.makedirs(os.path.dirname(self.__filePath))
        with open(self.__filePath, "wb") as f:
            f.write(os.urandom(self.__chunkSize))

    def tearDown(self):
        if os.path.exists(self.__unitTestFolder):
            shutil.rmtree(self.__unitTestFolder)

    def testContext(self):
        hashType = 'MD5'
        unit_test = True
        client_hash = None
        server_hash1 = None
        server_hash2 = None
        tempFilePath = None
        with open(self.__filePath, "rb") as r:
            h1 = hashlib.md5()
            h1.update(r.read())
            server_hash1 = h1.hexdigest()
        fao = self.__cU.getFileObject(self.__repoType, self.__depId, self.__contentType, self.__milestone, self.__partNumber, self.__contentFormat, self.__version, hashType, unit_test)
        with fao.clientContext as cc:
            tempFilePath = cc.name
            h2 = hashlib.md5()
            cc.seek(0)  # required before reading
            h2.update(cc.read())
            client_hash = h2.hexdigest()
            cc.seek(0)
            bytes = cc.read(64)
            # logger.info(bytes)
            cc.seek(0)
            cc.write(os.urandom(self.__chunkSize))
            self.assertTrue(os.path.exists(tempFilePath), 'error - file path does not exist')
        with open(self.__filePath, "rb") as r:
            h3 = hashlib.md5()
            h3.update(r.read())
            server_hash2 = h3.hexdigest()
        self.assertTrue(client_hash is not None, 'error - no client hash')
        self.assertTrue(server_hash1 is not None, 'error - no server hash1')
        self.assertTrue(server_hash2 is not None, 'error - no server hash2')
        self.assertTrue(client_hash == server_hash1, 'error - hash conflict in server hash1')
        self.assertTrue(client_hash != server_hash2, 'error - hash conflict in server hash2')
        self.assertFalse(os.path.exists(tempFilePath), 'error - file still exists')

    def testNonContext(self):
        hashType = 'MD5'
        unit_test = True
        fao = self.__cU.getFileObject(self.__repoType, self.__depId, self.__contentType, self.__milestone, self.__partNumber, self.__contentFormat, self.__version, hashType, unit_test)
        vals = "%s %s %s %s %s %s %s" % (fao.repositoryType,fao.depositId,fao.contentType,fao.milestone,fao.partNumber,fao.contentFormat,fao.version)
        self.assertTrue(vals == "unit-test D_000 model upload 1 pdbx 1")

def tests():
    suite = unittest.TestSuite()
    suite.addTest(TestClientContext('testContext'))
    suite.addTest(TestClientContext('testNonContext'))
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner()  # verbosity=0 for printing
    runner.run(tests())
