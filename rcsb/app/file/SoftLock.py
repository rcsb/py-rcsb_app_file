# file - SoftLock.py
# author - James Smith 2023

import asyncio
import signal
import sys
import time
import os
import socket
import glob
import uuid
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider

logging.basicConfig(level=logging.INFO)

# tasks - convert to random wait time to prevent simultaneously synchronized waiters


class Locking(object):
    """
    file-based lock where each request to one resource has its own file
    advantages - lock exit and cleanup have no race conditions ... just remove lock file
    disadvantages - requires directory traversal
                  - requires second wait to mitigate concurrency problems
    cooperative file lock that synchronizes transactions across multiple processes, machines, and containers
    only works across machines and containers if shared_lock_path in config.yml points to a remote server path
    if lock_transactions in config.yml = True, does nothing
    otherwise, creates unique locking file in shared locks directory for each lock request
    set timeout to desired max wait time, remembering that an asynchronous process is allowed to wait (so set high)
    set timeout = 0 to allow infinite wait
    requires one or two directory traversals, and one busy wait, so may be slow
    not ideal for chunked uploads/downloads or other forms of heavily repeated usage, unless race conditions are a risk
    for chunked downloads, might want to set second_traversal=False
    be aware that hash checks or file size comparisons are an alternative to locking or may complement locking
    server name and process id are written into lock file to stop process in event of infinite loop
    this module allows locking of file or directory that does not yet exist (simplifies usage when creating/overwriting)
    also does not create file or directory if source file/directory does not yet exist
    async required
    "async with Locking" statements should probably precede file open statements (to avoid opening file unless lock is acquired)
    example (exclusive)
    async with Locking(filepath, "w"):
        # do something with file, for example, create or overwrite
        async with aiofiles.open(filepath, "w") as w:
            await w.write(text)
    example (shared)
    async with Locking(filepath, "r")
        async with aiofiles.open(filepath, "r") as r:
            text = await r.read()
    test conditions:
        first traversal
        uselock set to false - do nothing
        shared lock - 1. finds nothing, 2. finds another shared lock, 3. finds exclusive lock
        exclusive lock - 4. finds nothing, 5. finds shared lock, 6. finds exclusive lock
        errors - max time exceeded
        second traversal - prevent simultaneous lock acquisitions
        shared lock - 1. finds nothing - acquire lock, 2. finds new shared lock - acquire lock, 3. finds new exclusive lock - defer to exclusive lock
        exclusive lock - 4. finds nothing - acquire lock, 5. finds new shared lock - acquire lock, 6. finds new exclusive lock - tiebreaker - alphabetical by uid
    """

    shared_lock_mode = "r"
    exclusive_lock_mode = "w"

    def __init__(self, filepath, mode, is_dir=False, timeout=60, second_traversal=True):
        logging.debug("initializing")
        provider = ConfigProvider()
        self.uselock = provider.get("LOCK_TRANSACTIONS")
        if bool(self.uselock) is False:
            logging.debug("use lock false, skipping file locks")
            return
        self.lockdir = provider.get("SHARED_LOCK_PATH")
        # target file path
        self.filepath = filepath  # might not exist
        # lock file path
        self.lockfilepath = None
        # written into lock file
        self.proc = os.getpid()
        self.hostname = str(socket.gethostname()).split(".")[0]
        # mode
        if mode not in [self.shared_lock_mode, self.exclusive_lock_mode]:
            raise OSError("error - unknown locking mode %s" % mode)
        self.mode = mode
        # whether lock is for a file or a directory
        self.is_dir = is_dir
        # time properties
        self.precision = 4
        self.start_time = round(time.time(), self.precision)
        # max seconds to wait to obtain a lock, set to zero for infinite wait
        self.timeout = timeout
        # appropriate wait time before second traversal is hard to predict, especially with async functions - can't be too long (so async ok)
        # ensure that this value does not conflict with or approach timeout value
        self.wait_before_second_traversal = 5  # if too high, slows chunked downloads, if too low, second traversal maybe pointless
        if second_traversal is None:
            second_traversal = True
        self.use_second_traversal = second_traversal
        logging.debug("initialized")

    async def __aenter__(self):
        if bool(self.uselock) is False:
            return
        logging.debug("attempting to get lock path for %s", self.filepath)
        if self.uselock is not None and self.mode is not None:
            # busy wait to acquire lock
            while True:
                try:
                    # traverse shared locks directory to determine lock file name
                    self.lockfilepath = self.getLockPath(self.filepath)
                    # process result
                    if self.lockfilepath is None:  # supposed to occur - 3, 5, 6
                        # wait on other lock
                        logging.debug("attempting to acquire lock on %s", self.filepath)
                        await asyncio.sleep(1)
                    else:
                        # create new lock file
                        if not os.path.exists(self.lockfilepath):  # 1, 2, 4
                            # do not make async or use await - otherwise, time before second traversal is unpredictable
                            with open(self.lockfilepath, "w", encoding="UTF-8") as w:
                                w.write("%d\n" % self.proc)
                                w.write("%s\n" % self.hostname)
                                w.write("%s\n" % self.start_time)
                            logging.info(
                                "acquired %s lock on %s",
                                self.mode,
                                os.path.basename(self.lockfilepath),
                            )
                        else:
                            # found same lock file, should not occur
                            # lock file names should be unique even for shared locks
                            logging.warning(
                                "error - lock file already exists %s", self.lockfilepath
                            )
                            raise FileExistsError("error - lock file already exists")
                        if not self.use_second_traversal:
                            break
                        # still have risk of two users requesting lock at exactly same time
                        # (in event of simultaneity)
                        # want predictable timing up to this point, afterwards doesn't matter
                        # if simultaneity has occurred, at this point both lock files are already created, so wait time consists of unknown overhead
                        # if lock files were created, neither process will be sleeping, so async effects are not an issue
                        await asyncio.sleep(self.wait_before_second_traversal)
                        # re-traverse to detect new race conditions
                        if not self.secondTraversal(self.lockfilepath):
                            # roll back locking transaction
                            if os.path.exists(self.lockfilepath):
                                os.unlink(self.lockfilepath)
                            # keep waiting
                            await asyncio.sleep(1)
                        else:
                            break
                except FileExistsError:
                    logging.warning("error - lock file already exists")
                    if self.lockfilepath is not None and os.path.exists(
                        self.lockfilepath
                    ):
                        try:
                            # comment out to test locking
                            os.unlink(self.lockfilepath)
                        except Exception:
                            logging.warning(
                                "error - could not remove lock file %s",
                                self.lockfilepath,
                            )
                    break
                except OSError:
                    logging.warning("unknown error in locking module")
                    if self.lockfilepath is not None and os.path.exists(
                        self.lockfilepath
                    ):
                        try:
                            # comment out to test locking
                            os.unlink(self.lockfilepath)
                        except Exception:
                            logging.warning(
                                "error - could not remove lock file %s",
                                self.lockfilepath,
                            )
                    break
                if self.timeout > 0 and time.time() - self.start_time > self.timeout:
                    logging.warning("lock timed out")
                    raise FileExistsError("lock timed out on %s" % self.filepath)

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        if bool(self.uselock) is False:
            return
        if self.lockfilepath is not None and os.path.exists(self.lockfilepath):
            try:
                # comment out to test locking
                os.unlink(self.lockfilepath)
            except Exception:
                logging.warning(
                    "error - could not remove lock file %s", self.lockfilepath
                )
        else:
            logging.warning(
                "warning - could not close lock file on %s",
                os.path.basename(self.lockfilepath),
            )
        if exc_type or exc_val or exc_tb:
            logging.warning("errors in exit lock for %s", self.lockfilepath)
            raise OSError("errors in exit lock")

    def getLockPath(self, filepath):
        """

        Args:
            filepath: str

        Returns: lock path (str)

        make lock path from target file path (not temp file path)
        lock filename - repositoryType~filename~mode~uid
        example (shared) - deposit~D_000_model_P1.cif.V1~r~a1234f13a4
        example (exclusive) - deposit~D_000_model_P1.cif.V1~w~a2f656a4f86
        lock directory - repositoryType~depId~mode~uid
        example (shared) - deposit~D_000~r~a1234f13a4
        example (exclusive) - deposit~D_000~w~a2f656a4f86
        """
        sharedLockDirPath = self.lockdir
        if not os.path.exists(sharedLockDirPath):
            os.makedirs(sharedLockDirPath)
        # does not test existence of filepath - allows securing file or dir before creation (when application does not know whether it exists yet)
        if self.is_dir:
            # example - /app/repository/deposit/D_000
            depId = os.path.basename(filepath)
            repositoryType = os.path.basename(os.path.dirname(filepath))
            basefilename = "%s~%s~%s" % (repositoryType, depId, self.mode)
        else:
            # example - /app/repository/deposit/D_000/D_000_model_P1.cif.V1
            filename = os.path.basename(filepath)
            depId = os.path.basename(os.path.dirname(filepath))
            repositoryType = os.path.basename(
                os.path.dirname(os.path.dirname(filepath))
            )
            basefilename = "%s~%s~%s" % (repositoryType, filename, self.mode)
        lockPath = os.path.join(sharedLockDirPath, basefilename)
        uid = self.firstTraversal(lockPath)
        if uid is None:  # 3, 5, 6
            return None
        lockPath = "%s~%s" % (lockPath, uid)  # 1, 2, 4
        return lockPath

    def getLockStem(self, lockpath):
        # return maximum length string that overlaps with all locks for the same file
        basename = os.path.basename(lockpath)
        tokens = basename.split("~")
        repositoryType = tokens[0]
        filename = tokens[1]
        return "%s~%s" % (repositoryType, filename)

    def getLockUid(self, lockpath):
        basename = os.path.basename(lockpath)
        tokens = basename.split("~")
        return tokens[3]

    def firstTraversal(self, lockpath):
        dirname = os.path.dirname(lockpath)
        basename = self.getLockStem(lockpath)
        logging.debug("dirname %s basename %s", dirname, basename)
        for filepath in glob.iglob("%s/%s*" % (dirname, basename)):
            filename = os.path.basename(filepath)
            logging.debug("traversed to filename %s", filename)
            if filename.startswith(basename):
                this_mode = self.mode
                that_mode = filename.split("~")[2]
                logging.debug("this mode %s that mode %s", this_mode, that_mode)
                if this_mode == self.exclusive_lock_mode:  # 5, 6
                    # exclusive lock must wait on shared or exclusive lock
                    return None
                elif that_mode == self.exclusive_lock_mode:  # 3
                    # shared lock must wait on exclusive lock
                    return None
                else:  # 2
                    # shared lock found shared lock
                    # presumably won't find an exclusive lock, so return uid
                    # if prefer to traverse all files instead, replace with "continue" or "pass"
                    continue
        # traversed all files and found nothing, so acquire lock
        return uuid.uuid4().hex  # 1, 4

    def secondTraversal(self, lockfilepath):
        dirname = os.path.dirname(lockfilepath)
        basename = self.getLockStem(lockfilepath)
        this_lock_file = os.path.basename(self.lockfilepath)
        this_mode = self.mode
        logging.debug("dirname %s basename %s", dirname, basename)
        # traverse to detect new locks
        for filepath in glob.iglob("%s/%s*" % (dirname, basename)):
            filename = os.path.basename(filepath)
            logging.debug("filename %s", filename)
            if filename.startswith(basename) and filename != this_lock_file:
                that_mode = filename.split("~")[2]
                if that_mode == self.exclusive_lock_mode:
                    if this_mode == self.shared_lock_mode:
                        # shared lock defers to simultaneous exclusive lock - 3
                        return False
                    elif this_mode == self.exclusive_lock_mode:
                        # tiebreaker between simultaneous exclusive locks - 6
                        if self.getLockUid(lockfilepath) < self.getLockUid(filename):
                            # acquire lock
                            break
                        else:
                            return False
        # traversed all files and found nothing, so acquire lock
        return True  # 1, 2, 4, 5

    def getToken(self, tokname):
        # lock file name = repositoryType~filename~mode~uid
        # lock file contains proc \n hostname \n start time
        if tokname == "proc":
            return Locking.getLockProcess(self.lockfilepath)
        elif tokname == "hostname":
            return Locking.getLockHostname(self.lockfilepath)
        elif tokname == "start":
            return Locking.getLockStartTime(self.lockfilepath)
        lockfilename = os.path.basename(self.lockfilepath)
        tokens = lockfilename.split("~")
        repositoryType = tokens[0]
        filename = tokens[1]
        mode = tokens[2]
        uid = tokens[3]
        if tokens is not None and len(tokens) > 0:
            if tokname == "repositoryType":
                return repositoryType
            elif tokname == "filename":
                return filename
            if tokname == "mode":
                return mode
            elif tokname == "uid":
                return uid
        return None

    def hasLock(self):
        if not self.lockExists():
            return False
        return self.getLockUid(self.lockfilepath) == self.getToken("uid")

    def lockExists(self):
        return os.path.exists(self.lockfilepath)

    def lockIsReader(self):
        result = self.getToken("mode")
        return result is not None and result == self.shared_lock_mode

    def lockIsWriter(self):
        result = self.getToken("mode")
        return result is not None and result == self.exclusive_lock_mode

    @staticmethod
    def getLockProcess(lockpath):
        text = None
        proc = -1
        with open(lockpath, "r", encoding="UTF-8") as r:
            text = r.read()
        if text:
            lines = text.split("\n")
            proc = lines[0]
        return int(proc)

    @staticmethod
    def getLockHostname(lockpath):
        text = None
        hostname = ""
        with open(lockpath, "r", encoding="UTF-8") as r:
            text = r.read()
        if text:
            lines = text.split("\n")
            hostname = lines[1]
        return hostname

    @staticmethod
    def getLockStartTime(lockpath):
        text = None
        starttime = None
        with open(lockpath, "r", encoding="UTF-8") as r:
            text = r.read()
        if text:
            lines = text.split("\n")
            starttime = lines[2]
            starttime = starttime.rstrip()
        return starttime

    @staticmethod
    async def getLockProcessHostname(lockpath):
        text = None
        with open(lockpath, "r", encoding="UTF-8") as r:
            text = r.read()
        if text:
            lines = text.split("\n")
            proc = lines[0]
            hostname = lines[1]
            return int(proc), hostname
        return None, None

    # remove all lock files and processes
    @staticmethod
    async def cleanup(save_unexpired=False, timeout=60):
        cP = ConfigProvider()
        lockDir = cP.get("SHARED_LOCK_PATH")
        for lockfilename in os.listdir(lockDir):
            lockfilepath = os.path.join(lockDir, lockfilename)
            creation_time = os.path.getmtime(lockfilepath)
            # optionally skip over unexpired locks
            if save_unexpired and time.time() - creation_time <= timeout:
                continue
            pid, that_host_name = await Locking.getLockProcessHostname(lockfilepath)
            if pid and that_host_name:
                this_host_name = str(socket.gethostname()).split(".")[0]
                if this_host_name == that_host_name:
                    try:
                        os.kill(pid, signal.SIGSTOP)
                    except ProcessLookupError:
                        pass
            os.unlink(lockfilepath)


if __name__ == "__main__":
    if len(sys.argv) == 3:
        saveunexpired = bool(sys.argv[1])
        time_out = int(sys.argv[2])
        asyncio.run(Locking.cleanup(saveunexpired, time_out))
