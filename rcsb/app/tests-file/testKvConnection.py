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
        mapTable = cP.get("KV_MAP_TABLE_NAME")
        lockTable = cP.get("KV_LOCK_TABLE_NAME")
        kV = KvConnection(filePath, sessionTable, mapTable, lockTable)
        self.assertTrue(kV is not None)
        connection = kV.getConnection()
        self.assertTrue(connection is not None)
        connection.close()


if __name__ == "__main__":
    unittest.main()
