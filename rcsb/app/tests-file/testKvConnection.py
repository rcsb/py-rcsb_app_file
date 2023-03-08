##
# File:    testKvConnection.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
import os
import rcsb.app.config.setConfig  # noqa: F401
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvConnection import KvConnection

# HERE = os.path.abspath(os.path.dirname(__file__))
# TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

class KvConnectionTest(unittest.TestCase):

    def testConnection(self):
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        filePath = cP.get("KV_FILE_PATH")
        sessionTable = cP.get("KV_SESSION_TABLE_NAME")
        logTable = cP.get("KV_LOG_TABLE_NAME")
        kV = KvConnection(filePath, sessionTable, logTable)
        self.assertTrue(kV is not None)
        connection = kV.getConnection()
        self.assertTrue(connection is not None)
        connection.close()

if __name__ == "__main__":
    unittest.main()
