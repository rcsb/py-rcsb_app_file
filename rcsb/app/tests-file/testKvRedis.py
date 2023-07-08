##
# File:    testKvRedis.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#

import unittest
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvRedis import KvRedis


class KvRedisTest(unittest.TestCase):
    def testKv(self):
        cP = ConfigProvider()
        kV = KvRedis(cP)
        self.assertTrue(kV is not None)


if __name__ == "__main__":
    unittest.main()
