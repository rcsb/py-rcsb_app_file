##
# File:    testKvSqlite.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
import os
import rcsb.app.config.setConfig  # noqa: F401
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvSqlite import KvSqlite

# HERE = os.path.abspath(os.path.dirname(__file__))
# TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

class KvSqliteTest(unittest.TestCase):

    def testKv(self):
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        kV = KvSqlite(cP)
        self.assertTrue(kV is not None)
        kV.setSession("test", "result", "pass")
        self.assertTrue(kV.getSession("test", "result") == "pass")
        kV.clearTable(kV.sessionTable)

if __name__ == "__main__":
    unittest.main()
