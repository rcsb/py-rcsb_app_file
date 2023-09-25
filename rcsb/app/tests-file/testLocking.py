import asyncio
from threading import Timer
import os
import unittest
import logging
from rcsb.app.file.RedisLock import Locking as redisLock
from rcsb.app.file.TernaryLock import Locking as ternaryLock
from rcsb.app.file.SoftLock import Locking as softLock
from rcsb.app.file.PathProvider import PathProvider

logging.basicConfig(level=logging.INFO)


class LockTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # recommended by Python docs at https://docs.python.org/3/library/unittest.html
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
        await self.testLock(lock=redisLock)

    async def testTernaryLock(self):
        logging.info("---- TESTING TERNARY LOCK ----")
        await self.testLock(lock=ternaryLock)

    async def testSoftLock(self):
        logging.info("---- TESTING SOFT LOCK ----")
        await self.testLock(lock=softLock)

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

    async def testLock(self, lock):
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
            try:
                async with lock(filepath, w, timeout=5, second_traversal=True):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("writer error %r", err)
            self.assertFalse(testVal == 0, "write lock error - test val = 0")
            self.assertTrue(testVal == 1, "write lock error - test val %d" % testVal)

        # test two readers
        logging.info("TESTING TWO READERS")
        filepath = self.getNextFilePath()
        # second traversal required to detect simultaneity otherwise ternary lock has infinite loop
        async with lock(filepath, r, timeout=60, second_traversal=True):
            testVal = 1
            try:
                async with lock(filepath, r, timeout=5, second_traversal=True):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("reader error %r", err)
            self.assertFalse(testVal == 1, "read lock error - test val = 1")
            self.assertTrue(
                testVal == 0, "read lock error - non-zero test val %d" % testVal
            )

        # test reader writer
        logging.info("TESTING READER WRITER")
        filepath = self.getNextFilePath()
        # second traversal required to detect simultaneity otherwise ternary lock has infinite loop
        async with lock(filepath, r, timeout=60, second_traversal=True):
            testVal = 1
            try:
                async with lock(filepath, w, timeout=5, second_traversal=True):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("reader error %r", err)
            self.assertFalse(testVal == 0, "read lock error - test val = 0")
            self.assertTrue(testVal == 1, "read lock error - test val %d" % testVal)

        # test writer reader
        logging.info("TESTING WRITER READER")
        filepath = self.getNextFilePath()
        async with lock(filepath, w, timeout=60, second_traversal=True):
            testVal = 1
            try:
                async with lock(filepath, r, timeout=5, second_traversal=True):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("reader error %r", err)
            self.assertFalse(testVal == 0, "lock error - test val = 0")
            self.assertTrue(testVal == 1, "lock error - test val %d" % testVal)

        # DELAYED ACQUISITION TESTS
        # first client is cancelled with a delayed thread timer
        # second request waits, then gains access to lock after delay
        # should not throw errors

        # test delayed reader
        logging.info("TESTING DELAYED READER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=5)
        await l1.__aenter__()
        self.assertTrue(l1.lockExists(), "error - lock does not exist")
        await l1.__aexit__()
        await asyncio.sleep(1)
        self.assertFalse(l1.lockExists(), "error - lock still exists")

        # test delayed writer
        logging.info("TESTING DELAYED WRITER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, w, timeout=5)
        await l1.__aenter__()
        self.assertTrue(l1.lockExists(), "error - lock does not exist")
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

        # test delayed reader reader
        logging.info("TESTING DELAYED READER READER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=5)
        await l1.__aenter__()
        self.assertTrue(l1.lockExists(), "error - l1 does not exist")
        l2 = lock(filepath, r, timeout=10)
        await l2.__aenter__()
        self.assertTrue(l2.lockExists(), "error - l2 does not exist")
        await asyncio.sleep(1)
        await l1.__aexit__()
        await l2.__aexit__()
        await asyncio.sleep(1)
        self.assertFalse(l1.lockExists(), "error - l1 still exists")
        self.assertFalse(l2.lockExists(), "error - l2 still exists")

        def closeLockAsync(*args):
            for arg in args:
                asyncio.run(arg.__aexit__())

        # test thread timers on one lock

        # test read lock
        logging.info("TESTING THREAD TIMER ON ONE READ LOCK")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=20)
        await l1.__aenter__()
        t1 = Timer(1, closeLockAsync, [l1])
        t1.start()
        await asyncio.sleep(5)
        self.assertFalse(l1.lockExists(), "error - l1 still exists")
        t1.cancel()

        # test write lock
        logging.info("TESTING THREAD TIMER ON ONE WRITE LOCK")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, w, timeout=20)
        await l1.__aenter__()
        t1 = Timer(1, closeLockAsync, [l1])
        t1.start()
        await asyncio.sleep(5)
        self.assertFalse(l1.lockExists(), "error - l1 does not exist")
        t1.cancel()

        # test thread timers on two locks
        logging.info("TESTING THREAD TIMER ON TWO LOCKS")

        # test reader reader
        logging.info("TESTING DELAYED READER READER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=20)
        await l1.__aenter__()
        self.assertTrue(l1.hasLock(), "error - l1 does not have lock")
        self.assertTrue(l1.lockIsReader(), "error - l1 is not a reader lock")
        l2 = lock(filepath, r, timeout=60)
        await l2.__aenter__()
        await asyncio.sleep(1)
        self.assertTrue(l2.lockExists(), "error - l2 does not exist")
        self.assertTrue(l2.lockIsReader(), "error - l2 is not a reader lock")
        await l1.__aexit__()
        await l2.__aexit__()

        # test reader writer
        logging.info("TESTING DELAYED READER WRITER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, r, timeout=20)
        await l1.__aenter__()
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
        await l2.__aexit__()
        t1.cancel()

        # test writer writer
        logging.info("TESTING DELAYED WRITER WRITER")
        filepath = self.getNextFilePath()
        l1 = lock(filepath, w, timeout=20)
        await l1.__aenter__()
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
