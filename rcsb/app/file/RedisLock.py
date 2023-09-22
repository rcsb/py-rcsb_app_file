# file - RedisLock.py
# author - James Smith 2023

import asyncio
import os
import socket
import signal
import sys
import time
import uuid
import logging
from rcsb.app.file.KvBase import KvBase
from rcsb.app.file.ConfigProvider import ConfigProvider


class Locking(object):
    """
    root key = lock table name
    secondary keys based on file name to be locked
    values are a list of [modality, count, hostname, process number, start time, waitlist] in a string
    modality = -1 (writer), 0 (no one), > 0 (readers)
    count = number of lock holders
    throws FileExistsError or OSError (because most other lock packages use those error types)
    example (exclusive)
    try:
        async with Locking(filepath, "w"):
            async with aiofiles.open(filepath, "w") as w:
                await w.write(text)
    except (FileExistsError, OSError) as err:
        logging.error("error %r", err)
    example (shared)
    try:
        async with Locking(filepath, "r")
            async with aiofiles.open(filepath, "r") as r:
                text = await r.read()
    except (FileExistsError, OSError) as err:
        logging.error("error %r", err)
    """

    def __init__(self, filepath, mode, is_dir=False, timeout=60, second_traversal=True):
        provider = ConfigProvider()
        self.uselock = provider.get("LOCK_TRANSACTIONS")
        if bool(self.uselock) is False:
            logging.debug("use lock false, skipping file locks")
            return
        self.exclusive_lock_mode = "w"
        self.shared_lock_mode = "r"
        if mode not in [self.exclusive_lock_mode, self.shared_lock_mode]:
            raise OSError("error - unrecognized locking mode %s" % mode)
        # mode for internal use only - for public visibility, mode is implicit from the modality
        self.mode = mode
        # add zero for each property in the value - modality, count, hostname, process number, start time, waitlist
        self.start_val = "[0,0,0,0,0,0]"
        # redis preferred - sqlite only works on one machine
        self.kV = KvBase()
        if not self.kV:
            raise OSError("error - could not connect to database")
        # configuration
        self.start_time = time.time()
        self.wait_time = (
            1  # should not need random wait time due to Redis single threading?
        )
        self.timeout = timeout
        self.filename = None
        if not is_dir:
            self.repositoryType = os.path.basename(
                os.path.dirname(os.path.dirname(filepath))
            )
            self.depFolder = os.path.basename(os.path.dirname(filepath))
            self.filename = os.path.basename(filepath)
        else:
            self.repositoryType = os.path.basename(os.path.dirname(filepath))
            self.filename = os.path.basename(filepath)
        # unique key name shared by readers and writers
        self.keyname = "%s~%s" % (self.repositoryType, self.filename)
        self.proc = os.getpid()
        self.hostname = str(socket.gethostname()).split(".")[0]
        # waitlist value - fair lock for one writer
        self.uid = uuid.uuid4().hex
        # second wait to prevent simultaneity
        # because get + dec requests are not atomic, have chance of pseudo-simultaneity problem
        # example race condition - machine1 get mod = 0, machine2 get mod = 0, machine1 dec mod = -1, machine2 dec mod = -2
        # strategy - save count value, wait briefly after acquisition, test new count value, verify equals expected count value
        self.second_traversal = second_traversal
        self.second_wait_time = 3

    def getToken(self, tokname):
        indices = ["mod", "count", "host", "proc", "start", "waitlist"]
        index = None
        try:
            index = indices.index(tokname)
        except ValueError:
            return -1
        return self.kV.getLock(self.keyname, index)

    def getMod(self):
        return self.kV.getLock(self.keyname, 0)

    def incMod(self):
        self.kV.incLock(self.keyname, index=0, start_val=self.start_val)

    def decMod(self):
        self.kV.decLock(self.keyname, index=0, start_val=self.start_val)

    def getCount(self):
        return self.kV.getLock(self.keyname, 1)

    def incCount(self):
        self.kV.incLock(self.keyname, index=1, start_val=self.start_val)

    def decCount(self):
        self.kV.decLock(self.keyname, index=1, start_val=self.start_val)

    # atomic operations (prevent intervening simultaneous request between the two functions)

    def getModAndCount(self):
        mod, count = self.kV.getLock(self.keyname, 0, 1)
        return mod, count

    def incModIncCount(self):
        self.kV.incIncLock(self.keyname, 0, 1, self.start_val)

    def incModDecCount(self):
        self.kV.incDecLock(self.keyname, 0, 1, self.start_val)

    def decModDecCount(self):
        self.kV.decDecLock(self.keyname, 0, 1, self.start_val)

    def decModIncCount(self):
        self.kV.decIncLock(self.keyname, 0, 1, self.start_val)

    # waitlist functions

    def lockHasWaitList(self) -> bool:
        # find out from kv if lock is waitlisted
        index = 5
        s = str(self.kV.getLock(self.keyname, index))
        return bool(s != "-1")

    def getWaitList(self):
        # return kv lock waitlist value
        index = 5
        return self.kV.getLock(self.keyname, index)

    def reservedWaitList(self):
        # true if I reserved the lock, false otherwise
        return self.getWaitList() == self.uid

    def setWaitList(self):
        # set kv lock waitlist value
        index = 5
        return self.kV.setLock(self.keyname, self.uid, index, self.start_val)

    def resetWaitList(self):
        # reset kv waitlist value
        index = 5
        if not self.kV.getLock(self.keyname, 0):
            return False
        return self.kV.setLock(self.keyname, -1, index, self.start_val)

    async def __aenter__(self):
        if bool(self.uselock) is False:
            return
        try:
            if self.kV.getLock(self.keyname, 0) is None:
                # set modality
                self.kV.setLock(
                    key=self.keyname, val=0, index=0, start_val=self.start_val
                )
                # set count
                self.kV.setLock(
                    key=self.keyname, val=0, index=1, start_val=self.start_val
                )
                # set hostname
                self.kV.setLock(
                    key=self.keyname,
                    val=self.hostname,
                    index=2,
                    start_val=self.start_val,
                )
                # set process number
                self.kV.setLock(
                    key=self.keyname, val=self.proc, index=3, start_val=self.start_val
                )
                # set start time
                self.kV.setLock(
                    key=self.keyname,
                    val=self.start_time,
                    index=4,
                    start_val=self.start_val,
                )
                # set waitlist
                self.kV.setLock(
                    key=self.keyname, val=-1, index=5, start_val=self.start_val
                )
            # busy wait to acquire lock
            while True:
                if time.time() - self.start_time > self.timeout:
                    raise FileExistsError(
                        "error - lock timed out on %s" % self.filename
                    )
                # requesting shared lock
                if self.mode == self.shared_lock_mode:
                    # readers ok, writers will block
                    mod, count = self.getModAndCount()
                    if mod < 0:
                        # writer has lock
                        await asyncio.sleep(self.wait_time)
                        continue
                    elif self.lockHasWaitList():
                        # lock is waitlisted
                        if mod == 0:
                            logging.warning(
                                "error - lock is waitlisted %s but no one has the lock for %s",
                                self.getWaitList(),
                                self.keyname,
                            )
                            self.resetWaitList()
                        await asyncio.sleep(self.wait_time)
                        continue
                    else:
                        # reader or no one has lock, lock is not waitlisted by a writer
                        # acquire shared lock
                        # increment value to alert others, add reader to count
                        self.incModIncCount()
                        # wait briefly and verify expected value still remains
                        if self.second_traversal:
                            expected_count = count + 1
                            await asyncio.sleep(self.second_wait_time)
                            observed_count = self.getCount()
                            if observed_count < expected_count:
                                # simultaneous writer has acquired lock
                                # delete lock (for all clients)
                                self.stopLock()
                                raise FileExistsError(
                                    "error - simultaneous read writes"
                                )
                        break
                # requesting exclusive lock
                elif self.mode == self.exclusive_lock_mode:
                    # readers will block, writers will block
                    mod, count = self.getModAndCount()
                    if mod is None:
                        raise OSError(
                            "error - could not find keyname %s" % self.keyname
                        )
                    if mod != 0:
                        # reader or writer has lock
                        # try to claim next lock
                        if not self.lockHasWaitList():
                            self.setWaitList()
                        await asyncio.sleep(self.wait_time)
                        continue
                    elif (
                        count == 0
                        and not self.lockHasWaitList()
                        or self.reservedWaitList()
                    ):
                        # no one has lock, lock is not waitlisted or I waitlisted it
                        # acquire exclusive lock
                        # set negative mod value to alert others, add writer to count
                        self.decModIncCount()
                        # wait briefly and verify expected value still remains
                        if self.second_traversal:
                            expected_count = count + 1
                            await asyncio.sleep(self.second_wait_time)
                            observed_count = self.getCount()
                            if observed_count != expected_count:
                                # delete lock (for all clients)
                                self.stopLock()
                                raise FileExistsError(
                                    "error - simultaneous read writes"
                                )
                        break
                    else:
                        # lock is probably waitlisted by someone else
                        if count == 0:
                            logging.warning("count = 0 but modality = %d", mod)
                        await asyncio.sleep(self.wait_time)
                        continue
        except FileExistsError as err:
            raise FileExistsError("lock error %r" % err)
        except OSError as err:
            raise OSError("lock error %r" % err)
        finally:
            # if I waitlisted lock, reset waitlist value (unless second traversal deleted lock)
            if self.reservedWaitList():
                self.resetWaitList()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if bool(self.uselock) is False:
            return
        # comment out to test lock
        if self.mode == self.shared_lock_mode:
            if self.kV.getLock(self.keyname, 0) is not None:
                # reduce mod value
                self.decMod()
                # subtract reader from count
                self.decCount()
        elif self.mode == self.exclusive_lock_mode:
            if self.kV.getLock(self.keyname, 0) is not None:
                # return mod to zero
                self.incMod()
                # subtract writer from count
                self.decCount()
        # test if I had waitlist
        if self.reservedWaitList():
            self.resetWaitList()
        # remove lock if unused
        val = self.kV.getLock(self.keyname, 0)
        if val is not None and val == 0:
            self.kV.remLock(self.keyname)

    async def stopLock(self):
        """stop process and release resources"""
        self.kV.remLock(self.keyname)
        try:
            os.kill(self.proc, signal.SIGSTOP)
        except ProcessLookupError:
            pass

    # remove all lock files and processes
    @staticmethod
    async def cleanup(save_unexpired=False, timeout=60):
        kV = KvBase()
        # retrieve lock 'table'
        hashvar = kV.getLockAll()
        if not hashvar:
            logging.warning("error - could not find hash")
            return
        if not isinstance(hashvar, dict):
            # if not Redis then Sqlite
            if isinstance(hashvar, list):
                temp = {}
                for x in range(0, len(hashvar)):
                    tpl = hashvar[x]
                    key = tpl[0]
                    val = tpl[1]
                    temp[key] = val
                hashvar = temp
        # retrieve all locks
        keys = hashvar.keys()
        # remove locks
        for key in keys:
            lst = hashvar[key]
            if lst is None:
                continue
            lst = eval(lst)  # pylint: disable=W0123
            that_host_name = lst[2]
            pid = int(lst[3])
            creation_time = float(lst[4])
            # optionally skip over unexpired locks
            if save_unexpired and time.time() - creation_time <= timeout:
                continue
            kV.remLock(key)
            if pid and that_host_name:
                this_host_name = str(socket.gethostname()).split(".")[0]
                if this_host_name == that_host_name:
                    try:
                        os.kill(pid, signal.SIGSTOP)
                    except ProcessLookupError:
                        pass


if __name__ == "__main__":
    if len(sys.argv) == 3:
        saveunexpired = bool(sys.argv[1])
        time_out = int(sys.argv[2])
        asyncio.run(Locking.cleanup(saveunexpired, time_out))
