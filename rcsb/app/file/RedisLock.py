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
    values are a list of [lock count, hostname, process number, start time, waitlist]
    lock count = -1 (writer), 0 (no one), > 0 (readers)
    """

    def __init__(
        self, filepath, mode, is_dir=False, timeout=60, second_traversal=False
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
        self.wait_time = 1
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
        self.second_traversal = False
        # fair lock for one writer
        self.waitlist = (
            -1
        )  # though uuid is a hexadecimal, set -1 when no one is wait listed, otherwise set to uid of next user

    def isWaitListed(self):
        # find out from kv if lock is waitlisted by someone else
        index = 4
        return str(self.kV.getLock(self.keyname, index)) != "-1"

    def getWaitList(self):
        # return kv lock waitlist value
        # does not return self.waitlist value
        index = 4
        return self.kV.getLock(self.keyname, index)

    def setWaitList(self):
        # set both self.waitlist value and kv lock waitlist value
        index = 4
        self.waitlist = uuid.uuid4().hex
        return self.kV.setLock(self.keyname, self.waitlist, index, self.start_val)

    def resetWaitList(self):
        index = 4
        self.waitlist = -1
        return self.kV.setLock(self.keyname, self.waitlist, index, self.start_val)

    async def __aenter__(self):
        if bool(self.uselock) is False:
            return
        try:
            if self.kV.getLock(self.keyname) is None:
                self.kV.setLock(self.keyname, 0, 0, self.start_val)
                self.kV.setLock(self.keyname, self.hostname, 1, self.start_val)
                self.kV.setLock(self.keyname, self.proc, 2, self.start_val)
                self.kV.setLock(self.keyname, self.start_time, 3, self.start_val)
                self.kV.setLock(self.keyname, self.waitlist, 4, self.start_val)
            # blocking wait to acquire lock
            while True:
                if time.time() - self.start_time > self.timeout:
                    raise FileExistsError(
                        "error - lock timed out on %s" % self.filename
                    )
                # requesting shared lock
                if self.mode == self.shared_lock_mode:
                    # readers ok, writers will block
                    if self.kV.getLock(self.keyname) < 0:
                        # writer has lock
                        await asyncio.sleep(self.wait_time)
                        continue
                    else:
                        # reader or no one has lock
                        # first test for waitlist
                        if self.isWaitListed():
                            # lock is waitlisted
                            await asyncio.sleep(self.wait_time)
                            continue
                        # acquire shared lock
                        # increment value to alert others
                        self.kV.incLock(self.keyname, self.start_val)
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
                        if not self.isWaitListed():
                            self.setWaitList()
                        await asyncio.sleep(self.wait_time)
                        continue
                    else:
                        # no one has lock
                        # acquire exclusive lock
                        # set negative value to alert others
                        self.kV.decLock(self.keyname, self.start_val)
                        break
        except FileExistsError as err:
            raise FileExistsError("lock error %r" % err)
        except OSError as err:
            raise OSError("lock error %r" % err)
        finally:
            if self.waitlist != -1:
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
        if self.waitlist != -1:
            self.resetWaitList()
        # remove lock if unused
        val = self.kV.getLock(self.keyname)
        if val is not None and val == 0:
            self.kV.remLock(self.keyname)

    # remove all lock files and processes
    @staticmethod
    async def cleanup(save_unexpired=False, timeout=60):
        kV = KvBase()
        hash = kV.getLockAll()
        if not hash:
            logging.warning("error - could not find hash")
            return
        if not isinstance(hash, dict):
            if isinstance(hash, list):
                temp = {}
                for x in range(0, len(hash)):
                    tpl = hash[x]
                    key = tpl[0]
                    val = tpl[1]
                    temp[key] = val
                hash = temp
        keys = hash.keys()
        for key in keys:
            lst = hash[key]
            if lst is None:
                continue
            lst = eval(lst)
            lock_count = int(lst[0])
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
