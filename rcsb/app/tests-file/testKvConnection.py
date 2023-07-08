##
# File:    testKvConnection.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvConnection import KvConnection


class KvConnectionTest(unittest.TestCase):
    def testConnection(self):
        cP = ConfigProvider()
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
