##
# File:    testKvRedis.py
# Author:  James Smith
# Date:    Jan-2023
# Version: 0.001
#
import os
import subprocess
import time
import unittest
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvRedis import KvRedis


class KvRedisTest(unittest.TestCase):
    # comment out if running redis already
    # runs only once
    @classmethod
    def setUpClass(cls):
        # os.system("brew services start redis")
        subprocess.Popen(
            ["redis-server"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        time.sleep(5)

    # comment out if running redis already
    # runs only once
    @classmethod
    def tearDownClass(cls):
        # redis-server often has an extra process accompanying redis instance that is listed first
        os.system(
            "pid=`ps -e | grep redis-server | head -n2 | awk '{print $1}' ` && echo 'removing process ' $pid && kill $pid;"
        )

    def testKv(self):
        cP = ConfigProvider()
        kV = KvRedis(cP)
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
