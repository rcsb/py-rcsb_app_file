##
# File:    testKvConnection.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
import os
import rcsb.app.config.setConfig  # noqa: F401 pylint: disable=W0611
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvConnection import KvConnection


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
