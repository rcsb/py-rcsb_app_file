import asyncio
import os
import unittest
import logging
from rcsb.app.file.RedisLock import Locking as redisLock
from rcsb.app.file.TernaryLock import Locking as ternaryLock
from rcsb.app.file.SoftLock import Locking as softLock
from rcsb.app.file.PathProvider import PathProvider

logging.basicConfig(level=logging.INFO)


class LockTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def testRedisLock(self):
        logging.info("----- TESTING REDIS LOCK -----")
        asyncio.run(testLock(self, lock=redisLock))

    def testTernaryLock(self):
        logging.info("---- TESTING TERNARY LOCK ----")
        asyncio.run(testLock(self, lock=ternaryLock))

    def testSoftLock(self):
        logging.info("---- TESTING SOFT LOCK ----")
        asyncio.run(testLock(self, lock=softLock))

async def testLock(self, lock):
    testVal = -1
    repositoryType = "unit-test"
    depId = "D_000"
    contentType = "model"
    milestone = "upload"
    partNumber = 1
    contentFormat = "pdbx"
    version = 1
    folder = PathProvider().getDirPath(repositoryType, depId)
    self.assertTrue(folder is not None, "error - could not create folder")
    logging.info("folder %s", folder)
    filename = PathProvider().getFileName(
        depId, contentType, milestone, partNumber, contentFormat, version
    )
    self.assertTrue(filename is not None, "error - could not create file name")
    logging.info("file name %s", filename)
    filepath = os.path.join(folder, filename)
    logging.info("file path %s", filepath)
    # test two writers
    logging.info("TESTING TWO WRITERS")
    try:
        async with lock(filepath, "w", timeout=60):
            testVal = 1
            try:
                async with lock(filepath, "w", timeout=5):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("writer error %r", err)
            self.assertFalse(testVal == 0, "write lock error - test val = 0")
            self.assertTrue(
                testVal == 1, "write lock error - test val %d" % testVal
            )
    except (FileExistsError, OSError) as err:
        logging.warning("writer error %r", err)
    # test two readers
    logging.info("TESTING TWO READERS")
    version = 2
    filename = PathProvider().getFileName(
        depId, contentType, milestone, partNumber, contentFormat, version
    )
    filepath = os.path.join(folder, filename)
    try:
        # second traversal required to detect simultaneity otherwise ternary lock has infinite loop
        async with lock(filepath, "r", timeout=60, second_traversal=True):
            testVal = 1
            try:
                async with lock(filepath, "r", timeout=5, second_traversal=True):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("reader error %r", err)
            self.assertFalse(testVal == 1, "read lock error - test val = 1")
            self.assertTrue(
                testVal == 0, "read lock error - non-zero test val %d" % testVal
            )
    except (FileExistsError, OSError) as err:
        logging.warning("reader error %r", err)
    # test reader writer
    logging.info("TESTING READER WRITER")
    version = 3
    filename = PathProvider().getFileName(
        depId, contentType, milestone, partNumber, contentFormat, version
    )
    filepath = os.path.join(folder, filename)
    try:
        # second traversal required to detect simultaneity otherwise ternary lock has infinite loop
        async with lock(filepath, "r", timeout=60, second_traversal=True):
            testVal = 1
            try:
                async with lock(filepath, "w", timeout=5, second_traversal=True):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("reader error %r", err)
            self.assertFalse(testVal == 0, "read lock error - test val = 0")
            self.assertTrue(
                testVal == 1, "read lock error - test val %d" % testVal
            )
    except (FileExistsError, OSError) as err:
        logging.warning("reader error %r", err)
    # test writer reader
    logging.info("TESTING WRITER READER")
    version = 4
    filename = PathProvider().getFileName(
        depId, contentType, milestone, partNumber, contentFormat, version
    )
    filepath = os.path.join(folder, filename)
    try:
        async with lock(filepath, "w", timeout=60):
            testVal = 1
            try:
                async with lock(filepath, "r", timeout=5):
                    testVal = 0
            except (FileExistsError, OSError) as err:
                logging.warning("writer error %r", err)
            self.assertFalse(testVal == 0, "lock error - test val = 0")
            self.assertTrue(
                testVal == 1, "lock error - test val %d" % testVal
            )
    except (FileExistsError, OSError) as err:
        logging.warning("error %r", err)

def tests():
    suite = unittest.TestSuite()
    suite.addTest(LockTest("testRedisLock"))
    suite.addTest(LockTest("testTernaryLock"))
    suite.addTest(LockTest("testSoftLock"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(tests())
