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

logging.basicConfig(level=logging.INFO)


class Locking(object):
    """
    advantages - with enough atomic transactions, completely eliminates second wait
    disadvantages - redis variable itself requires locking (a sub-lock) or other techniques to avoid race conditions
                  - lock exit and cleanup could remove newly acquired lock for someone else unless address all scenarios
                  - atomic transactions require spreading logic across multiple modules (RedisLock.py, KvRedis.py)
    root key = lock table name
    secondary keys based on file name to be locked
    values are a list of [modality, count, hostname, process number, start time, waitlist] in a string
    modality = -1 (writer), 0 (no one), > 0 (readers)
    count = number of lock holders
    throws FileExistsError or OSError (because most other lock packages use those error types)
    example (exclusive)
    try:
        async with Locking(filepath, "w"):
            # this line will not run until and unless aenter completes
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

    exclusive_lock_mode = "w"
    shared_lock_mode = "r"

    def __init__(self, filepath, mode, is_dir=False, timeout=60, second_traversal=True):
        provider = ConfigProvider()
        self.uselock = provider.get("LOCK_TRANSACTIONS")
        if bool(self.uselock) is False:
            logging.debug("use lock false, skipping file locks")
            return
        if mode not in [self.exclusive_lock_mode, self.shared_lock_mode]:
            raise OSError("error - unrecognized locking mode %s" % mode)
        # mode for internal use only - for public visibility, mode is implicit from the modality
        self.mode = mode
        # add zero for each property in the value - modality, count, hostname, process number, start time, waitlist
        self.start_val = "[0,0,0,0,0,-1]"
        self.mod_index = 0
        self.count_index = 1
        self.host_index = 2
        self.proc_index = 3
        self.start_index = 4
        self.waitlist_index = 5
        # redis preferred - sqlite only works on one machine
        self.kV = KvBase()
        if not self.kV:
            raise OSError("error - could not connect to database")
        # configuration
        self.precision = 4
        self.start_time = round(time.time(), self.precision)
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
        # because get + dec requests are not atomic, have chance of race condition
        # example race condition - machine1 get mod = 0, machine2 get mod = 0, machine1 dec mod = -1, machine2 dec mod = -2
        # strategy - save count value, wait briefly after acquisition, test new count value, verify equals expected count value
        # only for writers - reader count is not relevant, for example two simultaneous readers (one reader could add, count 2, or drop, count 1)
        # for reader/writer and writer/reader combinations, rely on writer to address race conditions
        self.second_traversal = second_traversal
        self.second_wait_time = 3

    def initialize(self):
        # set modality
        self.kV.setLock(
            key=self.keyname, val=0, index=self.mod_index, start_val=self.start_val
        )
        # set count
        self.kV.setLock(
            key=self.keyname, val=0, index=self.count_index, start_val=self.start_val
        )
        # set hostname
        self.kV.setLock(
            key=self.keyname,
            val=self.hostname,
            index=self.host_index,
            start_val=self.start_val,
        )
        # set process number
        self.kV.setLock(
            key=self.keyname,
            val=self.proc,
            index=self.proc_index,
            start_val=self.start_val,
        )
        # set start time
        self.kV.setLock(
            key=self.keyname,
            val=self.start_time,
            index=self.start_index,
            start_val=self.start_val,
        )
        # set waitlist default value -1
        self.kV.setLock(
            key=self.keyname,
            val=-1,
            index=self.waitlist_index,
            start_val=self.start_val,
        )

    async def __aenter__(self):
        if bool(self.uselock) is False:
            return
        try:
            if self.kV.getLock(self.keyname, 0) is None:
                self.initialize()
            # busy wait to acquire lock
            while True:
                if time.time() - self.start_time > self.timeout:
                    raise FileExistsError(
                        "error - lock timed out on %s" % self.filename
                    )
                # requesting shared lock
                if self.mode == self.shared_lock_mode:
                    # readers ok, writers will block
                    result, mod, count = self.incIncIfNonNeg()
                    if result:
                        # reader or no one has lock, lock is not waitlisted by a writer
                        # already acquired shared lock
                        # already incremented mod to alert others, already added reader to count
                        logging.info("acquired shared lock on %s", self.keyname)
                        # do not set hostname and process id since multiple readers may hold lock
                        break
                    elif mod is None:
                        # lock owner has exited and removed lock so restart lock
                        self.initialize()
                        mod, count = self.getModAndCount()
                    elif mod < 0:
                        # writer has lock
                        if mod < -1:
                            raise OSError("error - illegal mod value %d" % mod)
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
                        raise OSError("unknown error")
                # requesting exclusive lock
                elif self.mode == self.exclusive_lock_mode:
                    # readers will block, writers will block
                    result, mod, count = self.decIncIfZero()
                    if result:
                        # no one has lock, lock is not waitlisted or I waitlisted it
                        # acquire exclusive lock
                        # set negative mod value to alert others, add writer to count
                        # dec mod inc count already completed
                        # reset waitlist value to allow others to reserve lock
                        self.resetWaitList()
                        # establish ownership
                        self.setHostnameProcessStart()
                        logging.info("acquired exclusive lock on %s", self.keyname)
                        break
                    elif mod is None:
                        # lock owner has exited and removed lock so restart lock
                        self.initialize()
                        mod, count = self.getModAndCount()
                    elif mod != 0:
                        # reader or writer has lock
                        if mod < -1:
                            raise OSError("error - illegal mod value %d" % mod)
                        # try to claim next lock
                        if not self.lockHasWaitList():
                            self.setWaitList()
                        await asyncio.sleep(self.wait_time)
                        continue
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
            # if I waitlisted lock, reset waitlist value
            if self.reservedWaitList():
                self.resetWaitList()

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        if bool(self.uselock) is False:
            return
        # comment out to test lock
        if self.mode == self.shared_lock_mode:
            if self.kV.getLock(self.keyname, 0) is not None:
                # reduce mod value
                # subtract reader from count
                self.decDecLock()
        elif self.mode == self.exclusive_lock_mode:
            if self.kV.getLock(self.keyname, 0) is not None:
                # increment mod to zero
                # subtract writer from count
                self.incDecLock()
        # test if I had waitlist
        if self.reservedWaitList():
            self.resetWaitList()
        # remove lock if unused
        self.remIfSafe()

    # utility functions

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

    async def stopLock(self):
        """delete redis key and stop process"""
        self.kV.remLock(self.keyname)
        try:
            os.kill(self.proc, signal.SIGSTOP)
        except ProcessLookupError:
            pass

    def setHostnameProcessStart(self):
        index = 2
        self.kV.setLock(self.keyname, self.hostname, index, self.start_val)
        index = 3
        self.kV.setLock(self.keyname, self.proc, index, self.start_val)
        index = 4
        self.kV.setLock(self.keyname, self.start_time, index, self.start_val)

    # atomic operations (prevent intervening simultaneous request between the two functions)

    def getModAndCount(self):
        mod, count = self.kV.getLock(self.keyname, self.mod_index, self.count_index)
        return mod, count

    def getModCountWaitlist(self):
        mod, count, waitlist = self.kV.getLock(
            self.keyname, self.mod_index, self.count_index, self.waitlist_index
        )
        return mod, count, waitlist

    def incIncLock(self):
        """increment mod and count (with optional alternate indices)"""
        self.kV.incIncLock(
            self.keyname, self.mod_index, self.count_index, self.start_val
        )

    def incDecLock(self):
        """inc mod dec count"""
        self.kV.incDecLock(
            self.keyname, self.mod_index, self.count_index, self.start_val
        )

    def decDecLock(self):
        """decrement mod and count"""
        self.kV.decDecLock(
            self.keyname, self.mod_index, self.count_index, self.start_val
        )

    def decIncLock(self):
        """dec mod inc count"""
        self.kV.decIncLock(
            self.keyname, self.mod_index, self.count_index, self.start_val
        )

    def incIncIfZero(self):
        """inc mod (index1) inc count (index2) if both are zero and lock is not waitlisted (index3) by someone else"""
        return self.kV.incIncIfZero(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def incDecIfZero(self):
        """inc dec if both are zero"""
        return self.kV.incDecIfZero(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def decIncIfZero(self):
        """dec inc if both are zero"""
        return self.kV.decIncIfZero(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def decDecIfZero(self):
        """dec dec if both are zero"""
        return self.kV.decDecIfZero(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def incIncIfNonNeg(self):
        """inc inc if both are positive"""
        return self.kV.incIncIfNonNeg(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def incDecIfNonNeg(self):
        """inc dec if both are positive"""
        return self.kV.incDecIfNonNeg(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def decIncIfNonNeg(self):
        """dec inc if both are positive"""
        return self.kV.decIncIfNonNeg(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def decDecIfNonNeg(self):
        """dec dec if both are positive"""
        return self.kV.decDecIfNonNeg(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
            self.start_val,
        )

    def remIfSafe(self):
        """atomic removal to avoid removing someone else's newly acquired lock"""
        return self.kV.remIfSafe(
            self.keyname,
            self.uid,
            self.mod_index,
            self.count_index,
            self.waitlist_index,
        )

    # waitlist functions

    def lockHasWaitList(self) -> bool:
        # find out from kv if lock is waitlisted
        index = self.waitlist_index
        s = str(self.kV.getLock(self.keyname, index))
        return bool(s != "-1")

    def getWaitList(self):
        # return kv lock waitlist value
        index = self.waitlist_index
        return self.kV.getLock(self.keyname, index)

    def reservedWaitList(self):
        # true if I reserved the lock, false otherwise
        return self.getWaitList() == self.uid

    def setWaitList(self):
        # set kv lock waitlist value
        index = self.waitlist_index
        return self.kV.setLock(self.keyname, self.uid, index, self.start_val)

    def resetWaitList(self):
        # reset kv waitlist value
        index = self.waitlist_index
        if not self.kV.getLock(self.keyname, 0):
            return False
        return self.kV.setLock(self.keyname, -1, index, self.start_val)

    # ownership test - exclusive lock only
    def hasLock(self):
        if not self.lockExists():
            return False
        host = self.kV.getLock(self.keyname, 2)
        proc = self.kV.getLock(self.keyname, 3)
        start = self.kV.getLock(self.keyname, 4)
        return (
            (host == self.hostname)
            and (str(proc) == str(self.proc))
            and (str(start) == str(self.start_time))
        )

    # ownership test for shared locks - result not guaranteed to prove ownership as no single process owns a shared lock
    def lockExists(self):
        return self.kV.getLock(self.keyname, 0) is not None

    def lockIsReader(self):
        index = 0  # modality
        mod = self.kV.getLock(self.keyname, index)
        return mod is not None and mod > 0

    def lockIsWriter(self):
        index = 0  # modality
        mod = self.kV.getLock(self.keyname, index)
        return mod is not None and mod < 0

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
