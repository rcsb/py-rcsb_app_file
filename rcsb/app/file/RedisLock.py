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


# tasks - second traversal, unit tests
# second traversal - thinks no one has lock so acquires (r or w), hasn't waited, wait to verify
# Redis is single threaded but still have pseudo-simultaneity (from queued server requests from different machines) so need second traversal
# (multiple writers simultaneously) if decLock happens multiple times, wait will detect invalid value (not -1)
# (reader and writer simultaneously) decLock and incLock cancel out to zero
# problem - could end up with unpredictable value
# strategy - set expected value, then wait and verify still has expected value
# if second wait detects problem, keep lock and try to correct, or delete?

class Locking(object):
    """
    root key = lock table name
    secondary keys based on file name to be locked
    values are a list of [lock count, hostname, process number, start time, waitlist]
    lock count = -1 (writer), 0 (no one), > 0 (readers)
    """

    def __init__(
        self, filepath, mode, is_dir=False, timeout=60, second_traversal=True
    ):
        provider = ConfigProvider()
        self.uselock = provider.get("LOCK_TRANSACTIONS")
        if bool(self.uselock) is False:
            logging.debug("use lock false, skipping file locks")
            return
        self.exclusive_lock_mode = "w"
        self.shared_lock_mode = "r"
        if mode not in [self.exclusive_lock_mode, self.shared_lock_mode]:
            raise OSError("error - unrecognized locking mode %s" % mode)
        # mode for internal use only - for public visibility, mode is implicit from the lock count
        self.mode = mode
        # add zero for each property in the value
        self.start_val = "[0,0,0,0,0]"
        # redis preferred - sqlite only works on one machine
        self.kV = KvBase()
        if not self.kV:
            raise OSError("error - could not connect to database")
        # configuration
        self.start_time = time.time()
        self.wait_time = 1  # should not need random wait time due to single-threading?
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
        self.second_traversal = second_traversal
        self.second_wait_time = 3

    def lockHasWaitList(self) -> bool:
        # find out from kv if lock is waitlisted
        index = 4
        s = str(self.kV.getLock(self.keyname, index))
        return bool(s != "-1")

    def getWaitList(self):
        # return kv lock waitlist value
        index = 4
        return self.kV.getLock(self.keyname, index)

    def reservedWaitList(self):
        # true if I reserved the lock, false otherwise
        return self.getWaitList() == self.uid

    def setWaitList(self):
        # set kv lock waitlist value
        index = 4
        return self.kV.setLock(self.keyname, self.uid, index, self.start_val)

    def resetWaitList(self):
        # reset kv waitlist value
        index = 4
        if not self.kV.getLock(self.keyname):
            return False
        return self.kV.setLock(self.keyname, -1, index, self.start_val)

    async def __aenter__(self):
        if bool(self.uselock) is False:
            return
        try:
            if self.kV.getLock(self.keyname) is None:
                self.kV.setLock(self.keyname, 0, 0, self.start_val)
                self.kV.setLock(self.keyname, self.hostname, 1, self.start_val)
                self.kV.setLock(self.keyname, self.proc, 2, self.start_val)
                self.kV.setLock(self.keyname, self.start_time, 3, self.start_val)
                self.kV.setLock(self.keyname, -1, 4, self.start_val)
            # blocking wait to acquire lock (server may suspend, but code execution will wait for outcome of lock)
            waited = False
            while True:
                if time.time() - self.start_time > self.timeout:
                    raise FileExistsError(
                        "error - lock timed out on %s" % self.filename
                    )
                # requesting shared lock
                if self.mode == self.shared_lock_mode:
                    # readers ok, writers will block
                    count = self.kV.getLock(self.keyname)
                    if count < 0:
                        # writer has lock
                        await asyncio.sleep(self.wait_time)
                        continue
                    elif self.lockHasWaitList():
                        # lock is waitlisted
                        if count == 0:
                            logging.warning(
                                "error - lock is waitlisted %s but no one has the lock for %s",
                                self.getWaitList(),
                                self.keyname,
                            )
                            self.resetWaitList()
                        await asyncio.sleep(self.wait_time)
                        continue
                    else:
                        # reader or no one has lock
                        # lock is not waitlisted by a writer
                        # acquire shared lock
                        # increment value to alert others
                        self.kV.incLock(self.keyname, self.start_val)
                        # if haven't had to wait yet, wait briefly and verify expected value still remains
                        if self.second_traversal and not waited:
                            expected_value = count + 1
                            await asyncio.sleep(self.second_wait_time)
                            count = self.kV.getLock(self.keyname)
                            if count != expected_value:
                                # delete lock (for all clients)
                                self.stopLock()
                                raise FileExistsError("error - simultaneous read writes")
                        break
                # requesting exclusive lock
                elif self.mode == self.exclusive_lock_mode:
                    # readers will block, writers will block
                    count = self.kV.getLock(self.keyname)
                    if count is None:
                        raise OSError(
                            "error - could not find keyname %s" % self.keyname
                        )
                    if count != 0:
                        # reader or writer has lock
                        # try to claim next lock
                        if not self.lockHasWaitList():
                            self.setWaitList()
                        await asyncio.sleep(self.wait_time)
                        continue
                    elif not self.lockHasWaitList() or self.reservedWaitList():
                        # no one has lock
                        # lock is not waitlisted or I waitlisted it
                        # acquire exclusive lock
                        # set negative value to alert others
                        self.kV.decLock(self.keyname, self.start_val)
                        # if haven't had to wait yet, wait briefly and verify expected value still remains
                        if self.second_traversal and not waited:
                            expected_value = count - 1
                            await asyncio.sleep(self.second_wait_time)
                            count = self.kV.getLock(self.keyname)
                            if count != expected_value:
                                # delete lock (for all clients)
                                self.stopLock()
                                raise FileExistsError("error - simultaneous read writes")
                        break
                    else:
                        # lock is waitlisted by someone else
                        await asyncio.sleep(self.wait_time)
                        continue
                waited = True
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
            if self.kV.getLock(self.keyname) is not None:
                self.kV.decLock(self.keyname, self.start_val)
        elif self.mode == self.exclusive_lock_mode:
            if self.kV.getLock(self.keyname) is not None:
                self.kV.incLock(self.keyname, self.start_val)
        # test if I had waitlist
        if self.reservedWaitList():
            self.resetWaitList()
        # remove lock if unused
        val = self.kV.getLock(self.keyname)
        if val is not None and val == 0:
            self.kV.remLock(self.keyname)

    async def stopLock(self):
        """ stop process and release resources
        """
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
            that_host_name = lst[1]
            pid = int(lst[2])
            creation_time = float(lst[3])
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
