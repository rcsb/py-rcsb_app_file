import asyncio
from threading import Timer
import os
import sys
import unittest
import logging
from rcsb.app.file.PathProvider import PathProvider
from rcsb.app.file.KvBase import KvBase
from rcsb.app.file.ConfigProvider import ConfigProvider

# must start Redis server prior to test

provider = ConfigProvider()
kvmode = provider.get("KV_MODE")
if kvmode == "redis":
    from rcsb.app.file.RedisLock import Locking as redisLock
else:
    from rcsb.app.file.RedisSqliteLock import Locking as redisLock
from rcsb.app.file.TernaryLock import Locking as ternaryLock
from rcsb.app.file.SoftLock import Locking as softLock

logging.basicConfig(level=logging.INFO)


class LockTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.test = 0
        cp = ConfigProvider()
        kv = KvBase(cp)
        kv.clearTable(cp.get("KV_LOCK_TABLE_NAME"))
        self.locktype = cp.get("LOCK_TYPE")
        # following Python docs at https://docs.python.org/3/library/unittest.html
        # pylint prefers __init__
        self.repositoryType = "unit-test"  # pylint: disable=W0201
        self.depId = "D_000"  # pylint: disable=W0201
        self.contentType = "model"  # pylint: disable=W0201
        self.milestone = "upload"  # pylint: disable=W0201
        self.partNumber = 1  # pylint: disable=W0201
        self.contentFormat = "pdbx"  # pylint: disable=W0201
        self.version = 0  # pylint: disable=W0201

    async def asyncTearDown(self) -> None:
        pass

    async def testRedisLock(self):
        logging.info("----- TESTING REDIS LOCK -----")
        self.test = 1
        await self.testLock()

    async def testTernaryLock(self):
        logging.info("---- TESTING TERNARY LOCK ----")
        self.test = 2
        await self.testLock()

    async def testSoftLock(self):
        logging.info("---- TESTING SOFT LOCK ----")
        self.test = 3
        await self.testLock()

    def getNextFilePath(self):
        folder = PathProvider().getDirPath(self.repositoryType, self.depId)
        self.version += 1
        filename = PathProvider().getFileName(
            self.depId,
            self.contentType,
            self.milestone,
            self.partNumber,
            self.contentFormat,
            self.version,
        )
        filepath = os.path.join(folder, filename)
        return filepath

    async def testLock(self):
        # self.test += 1
        if self.test == 1:
            lock = redisLock
        elif self.test == 2:
            lock = ternaryLock
        elif self.test == 3:
            lock = softLock
        else:
            sys.exit("test = %d" % self.test)
        testVal = -1
        r = lock.shared_lock_mode
        w = lock.exclusive_lock_mode

        # LOCK DENIAL TESTS
        # second request is denied access to lock due to timeout
        # should throw error when second lock times out

        # test two writers
        logging.info("TESTING TWO WRITERS")
        filepath = self.getNextFilePath()
        logging.info("file path %s", filepath)
        async with lock(filepath, w, timeout=60, second_traversal=True):
            testVal = 1
            with self.assertRaises(FileExistsError):
                async with lock(filepath, w, timeout=5, second_traversal=True):
                    testVal = 0
            self.assertFalse(testVal == 0, "lock error - test val = 0")
            self.assertTrue(testVal == 1, "lock error - test val %d" % testVal)

        # test two readers
        logging.info("TESTING TWO READERS")
        filepath = self.getNextFilePath()
        # second traversal required to detect simultaneity otherwise ternary lock has infinite loop
        async with lock(filepath, r, timeout=60, second_traversal=True):
            testVal = 1
            # should not throw error
            async with lock(filepath, r, timeout=5, second_traversal=True):
                testVal = 0
            self.assertFalse(testVal == 1, "lock error - test val = 1")
            self.assertTrue(testVal == 0, "lock error - non-zero test val %d" % testVal)

        # test reader writer
        logging.info("TESTING READER WRITER")
        filepath = self.getNextFilePath()
        # second traversal required to detect simultaneity otherwise ternary lock has infinite loop
        async with lock(filepath, r, timeout=60, second_traversal=True):
            testVal = 1
            with self.assertRaises(FileExistsError):
                async with lock(filepath, w, timeout=5, second_traversal=True):
                    testVal = 0
            self.assertFalse(testVal == 0, "lock error - test val = 0")
            self.assertTrue(testVal == 1, "lock error - test val %d" % testVal)

        # test writer reader
        logging.info("TESTING WRITER READER")
        filepath = self.getNextFilePath()
        async with lock(filepath, w, timeout=60, second_traversal=True):
            testVal = 1
            with self.assertRaises(FileExistsError):
                async with lock(filepath, r, timeout=5, second_traversal=True):
                    testVal = 0
            self.assertFalse(testVal == 0, "lock error - test val = 0")
            self.assertTrue(testVal == 1, "lock error - test val %d" % testVal)

        # tests without async-with

        # test reader
        logging.info("TESTING READER MANUALLY")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=5)
        await l1.__aenter__()
        # readers do not own lock so can't test ownership
        self.assertTrue(l1.lockExists(), "error - lock does not exist")
        if lock == redisLock:
            self.assertTrue(
                l1.getMod() == 1 and l1.getCount() == 1,
                "error - mod %d count %d" % (l1.getMod(), l1.getCount()),
            )
        await l1.__aexit__()
        await asyncio.sleep(1)
        self.assertFalse(l1.lockExists(), "error - lock still exists")

        # test writer
        logging.info("TESTING WRITER MANUALLY")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, w, timeout=5)
        await l1.__aenter__()
        self.assertTrue(l1.lockExists(), "error - lock does not exist")
        if lock == redisLock:
            self.assertTrue(
                l1.getMod() == -1 and l1.getCount() == 1,
                "error - mod %d count %d" % (l1.getMod(), l1.getCount()),
            )
        # writer should own lock so test ownership
        self.assertTrue(
            l1.hasLock(),
            "error - l1 %s %s %s does not have lock %s %s %s"
            % (
                l1.hostname,
                l1.proc,
                l1.start_time,
                l1.getToken("hostname"),
                l1.getToken("proc"),
                l1.getToken("start"),
            ),
        )
        await l1.__aexit__()
        await asyncio.sleep(1)
        self.assertFalse(l1.lockExists(), "error - lock still exists")

        # test two readers
        logging.info("TESTING READER READER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=5)
        await l1.__aenter__()
        self.assertTrue(l1.lockExists(), "error - l1 does not exist")
        if lock == redisLock:
            self.assertTrue(
                l1.getMod() == 1 and l1.getCount() == 1,
                "error - mod %d count %d" % (l1.getMod(), l1.getCount()),
            )
        l2 = lock(filepath, r, timeout=10)
        await l2.__aenter__()
        # with reader lock, could still be l1
        self.assertTrue(l2.lockExists(), "error - l2 does not exist")
        # verify that l2 obtained lock (l1 should return same results also)
        if lock == redisLock:
            self.assertTrue(
                l2.getMod() == 2 and l2.getCount() == 2,
                "error - l2 mod %d count %d" % (l2.getMod(), l2.getCount()),
            )
            self.assertTrue(
                l1.getMod() == 2 and l1.getCount() == 2,
                "error - l1 mod %d count %d" % (l1.getMod(), l1.getCount()),
            )
        await l1.__aexit__()
        await l2.__aexit__()
        await asyncio.sleep(1)
        self.assertFalse(l1.lockExists(), "error - l1 still exists")
        self.assertFalse(l2.lockExists(), "error - l2 still exists")

        # DELAYED ACQUISITION TESTS
        # first client is cancelled with a delayed thread timer
        # second request waits, then gains access to lock after delay
        # should not throw errors

        def closeLockAsync(*args):
            for arg in args:
                asyncio.run(arg.__aexit__())

        # test thread timers on one lock
        logging.info("TESTING THREAD TIMER ON ONE LOCK")

        # test read lock
        logging.info("TESTING READ LOCK")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=20)
        await l1.__aenter__()
        t1 = Timer(1, closeLockAsync, [l1])
        t1.start()
        await asyncio.sleep(5)
        self.assertFalse(l1.lockExists(), "error - l1 still exists")
        t1.cancel()

        # test write lock
        logging.info("TESTING WRITE LOCK")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, w, timeout=20)
        await l1.__aenter__()
        t1 = Timer(1, closeLockAsync, [l1])
        t1.start()
        await asyncio.sleep(5)
        self.assertFalse(l1.lockExists(), "error - l1 still exists")
        t1.cancel()

        # test thread timers on two locks
        logging.info("TESTING THREAD TIMER ON TWO LOCKS")

        # test reader reader
        logging.info("TESTING DELAYED READER READER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=20)
        await l1.__aenter__()
        self.assertTrue(l1.lockIsReader(), "error - l1 is not a reader lock")
        t1 = Timer(3, closeLockAsync, [l1])
        t1.start()
        l2 = lock(filepath, r, timeout=60)
        await l2.__aenter__()
        # close first lock
        t1.join()
        await asyncio.sleep(1)
        # verify second lock still exists
        self.assertTrue(l2.lockExists(), "error - l2 does not exist")
        self.assertTrue(l2.lockIsReader(), "error - l2 is not a reader lock")
        # verify first lock closed
        if lock == redisLock:
            self.assertTrue(
                l2.getMod() == 1 and l2.getCount() == 1,
                "error - mod %d count %d" % (l2.getMod(), l2.getCount()),
            )
        await l2.__aexit__()
        t1.cancel()

        # test reader writer
        logging.info("TESTING DELAYED READER WRITER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=20)
        await l1.__aenter__()
        self.assertTrue(l1.hasLock(), "error - l1 does not have lock")
        self.assertTrue(l1.lockIsReader(), "error - l1 is not a reader lock")
        t1 = Timer(3, closeLockAsync, [l1])
        t1.start()
        l2 = lock(filepath, w, timeout=60)
        await l2.__aenter__()
        t1.join()
        await asyncio.sleep(5)
        self.assertTrue(l2.lockExists(), "error - l2 does not exist")
        self.assertTrue(
            l2.hasLock(),
            "error - l2 does not have lock, l2 %s %s %s lock %s %s %s"
            % (
                l2.hostname,
                l2.proc,
                l2.start_time,
                l2.getToken("host"),
                l2.getToken("proc"),
                l2.getToken("start"),
            ),
        )
        self.assertTrue(l2.lockIsWriter(), "error - l2 is not a writer lock")
        if lock == redisLock:
            self.assertTrue(
                l2.getMod() == -1 and l2.getCount() == 1,
                "error - mod %d count %d" % (l2.getMod(), l2.getCount()),
            )
        await l2.__aexit__()
        t1.cancel()

        # test writer writer
        logging.info("TESTING DELAYED WRITER WRITER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, w, timeout=20)
        await l1.__aenter__()
        self.assertTrue(l1.hasLock(), "error - l1 does not have lock")
        self.assertTrue(l1.lockIsWriter(), "error - l1 is not a writer lock")
        t1 = Timer(3, closeLockAsync, [l1])
        t1.start()
        l2 = lock(filepath, w, timeout=60)
        await l2.__aenter__()
        t1.join()
        await asyncio.sleep(5)
        self.assertTrue(l2.lockExists(), "error - l2 does not exist")
        self.assertTrue(
            l2.hasLock(),
            "error - l2 does not have lock, l2 %s %s %s lock %s %s %s"
            % (
                l2.hostname,
                l2.proc,
                l2.start_time,
                l2.getToken("host"),
                l2.getToken("proc"),
                l2.getToken("start"),
            ),
        )
        self.assertTrue(l2.lockIsWriter(), "error - l2 is not a writer lock")
        if lock == redisLock:
            self.assertTrue(
                l2.getMod() == -1 and l2.getCount() == 1,
                "error - mod %d count %d" % (l2.getMod(), l2.getCount()),
            )
        await l2.__aexit__()
        t1.cancel()

        # test writer reader
        logging.info("TESTING DELAYED WRITER READER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, w, timeout=20)
        await l1.__aenter__()
        self.assertTrue(l1.hasLock(), "error - l1 does not have lock")
        self.assertTrue(l1.lockIsWriter(), "error - l1 is not a writer lock")
        t1 = Timer(3, closeLockAsync, [l1])
        t1.start()
        l2 = lock(filepath, r, timeout=60)
        await l2.__aenter__()
        t1.join()
        await asyncio.sleep(5)
        self.assertTrue(l2.lockExists(), "error - l2 does not exist")
        self.assertTrue(l2.lockIsReader(), "error - l2 is not a reader lock")
        await l2.__aexit__()
        t1.cancel()


def tests():
    suite = unittest.TestSuite()
    suite.addTest(LockTest("testRedisLock"))
    suite.addTest(LockTest("testTernaryLock"))
    suite.addTest(LockTest("testSoftLock"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(tests())
