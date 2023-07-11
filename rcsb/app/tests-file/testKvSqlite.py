##
# File:    testKvSqlite.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvSqlite import KvSqlite

logging.basicConfig(level=logging.INFO)


class KvSqliteTest(unittest.TestCase):
    def testKv(self):
        cP = ConfigProvider()
        kV = KvSqlite(cP)
        self.assertTrue(kV is not None)
        # test map table
        kV.setMap("test", "pass")
        self.assertTrue(kV.getMap("test") == "pass")
        kV.clearMapVal("pass")
        kV.clearTable(kV.mapTable)
        # test sessions table
        kV.setSession("test", "result", "pass")
        self.assertTrue(kV.getSession("test", "result") == "pass")
        kV.clearSessionVal("test", "result")
        kV.clearSessionKey("test")
        kV.clearTable(kV.sessionTable)


if __name__ == "__main__":
    unittest.main()
