##
# File:    testKvSqlite.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
import os
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvSqlite import KvSqlite


class KvSqliteTest(unittest.TestCase):

    def testKv(self):
        cP = ConfigProvider()
        kV = KvSqlite(cP)
        self.assertTrue(kV is not None)
        kV.setSession("test", "result", "pass")
        self.assertTrue(kV.getSession("test", "result") == "pass")
        kV.clearTable(kV.sessionTable)

if __name__ == "__main__":
    unittest.main()
