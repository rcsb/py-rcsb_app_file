##
# File:    testKvRedis.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
import os
import rcsb.app.config.setConfig  # noqa: F401 pylint: disable=W0611
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvRedis import KvRedis


class KvRedisTest(unittest.TestCase):

    def testKv(self):
        configFilePath = os.environ.get("CONFIG_FILE")
        cP = ConfigProvider(configFilePath)
        kV = KvRedis(cP)
        self.assertTrue(kV is not None)

if __name__ == "__main__":
    unittest.main()
