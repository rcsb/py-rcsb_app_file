##
# File:    testKvRedis.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
import os
import rcsb.app.config.setConfig
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvRedis import KvRedis

# HERE = os.path.abspath(os.path.dirname(__file__))
# TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", os.path.join(TOPDIR, "rcsb", "app", "config", "config.yml"))

class KvRedisTest(unittest.TestCase):

    def testKv(self):
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        kV = KvRedis(cP)
        self.assertTrue(kV is not None)

if __name__ == "__main__":
    unittest.main()
