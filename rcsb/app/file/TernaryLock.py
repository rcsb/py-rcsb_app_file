# file - TernaryLock.py
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
    same as soft lock with added ability for writer to access heavily read files
    ternary lock - has three modes - exclusive, shared, and transitory
    the transitory mode is only for internal use - prevents bug where endless readers block writer access
    instead, writer waiting on a lock gets a transitory lock that essentially queues them as next in line for the lock
    transitory lock could also be implemented for the reverse situation, or both, but haven't done so here
    throws FileExistsError or OSError
    example (exclusive)
    try:
        async with Locking(filepath, "w"):
            # do something with file, for example, create or overwrite
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

    shared_lock_mode = "r"
    exclusive_lock_mode = "w"
    transitory_lock_mode = "t"  # internal use only

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
        if not is_dir:
            self.repositoryType = os.path.basename(
                os.path.dirname(os.path.dirname(filepath))
            )
            self.depFolder = os.path.basename(os.path.dirname(filepath))
            self.filename = os.path.basename(filepath)
        else:
            self.repositoryType = os.path.basename(os.path.dirname(filepath))
            self.filename = os.path.basename(filepath)
        # lock file path
        self.lockfilename = None
        self.uid = None
        # written into lock file
        self.proc = os.getpid()
        self.hostname = str(socket.gethostname()).split(".")[0]
        # mode
        if mode not in [self.shared_lock_mode, self.exclusive_lock_mode]:
            raise OSError("error - unknown locking mode %s" % mode)
        self.mode = mode
        self.start_mode = mode
        # whether lock is for a file or a directory
        self.is_dir = is_dir
        # time properties
        self.precision = 4
        self.start_time = round(time.time(), self.precision)
        self.wait_time = 1  # seconds
        # max seconds to wait to obtain a lock, set to zero for infinite wait
        self.timeout = timeout
        # attempt to prevent simultaneity
        if second_traversal is None:  # default not working sometimes
            second_traversal = True
        self.second_traversal = second_traversal
        self.wait_before_second_traversal = 3
        logging.debug("initialized")

    async def __aenter__(self):
        if bool(self.uselock) is False:
            return
        try:
            # busy wait to acquire target lock
            while True:
                no_conflicts_found = True
                found_nothing = True
                # find overlaps with other lock files
                # lock file name = repositoryType~filename~mode~uid
                thisfile = "%s~%s" % (self.repositoryType, self.filename)
                logging.debug("dirname %s basename %s", self.lockdir, thisfile)
                # traverse and evaluate conflicts
                # if find nothing or resolve conflict, acquire target lock
                # if find conflict, wait (and possibly acquire transitory lock) and traverse again
                for thatpath in glob.iglob("%s/%s*" % (self.lockdir, thisfile)):
                    found_nothing = False
                    thatfile = os.path.basename(thatpath)
                    logging.debug("traversed to filename %s", thatfile)
                    if thatfile.startswith(thisfile) and thatfile != self.lockfilename:
                        # found different lock on same file, may have conflict
                        that_mode = thatfile.split("~")[2]
                        logging.debug(
                            "this mode %s start mode %s that mode %s",
                            self.mode,
                            self.start_mode,
                            that_mode,
                        )
                        # I requested shared lock
                        if self.start_mode == self.shared_lock_mode:
                            if (
                                that_mode == self.exclusive_lock_mode
                                or that_mode == self.transitory_lock_mode
                            ):
                                # wait, then traverse again
                                no_conflicts_found = False
                                break  # from for loop
                            elif that_mode == self.shared_lock_mode:
                                # traverse all matching files, if just find shared locks then acquire lock
                                continue
                            else:
                                raise OSError(
                                    "error - unknown lock mode %s" % that_mode
                                )
                        # I requested exclusive lock
                        elif self.start_mode == self.exclusive_lock_mode:
                            # found exclusive or shared lock, must wait
                            if (
                                that_mode == self.exclusive_lock_mode
                                or that_mode == self.shared_lock_mode
                            ):
                                no_conflicts_found = False
                                # I have transitory lock?
                                if self.mode == self.transitory_lock_mode:
                                    # if yes, wait, then traverse again
                                    break  # from for loop
                                else:
                                    # if no, acquire transitory lock, then keep waiting
                                    self.mode = self.transitory_lock_mode
                                    # make new transitory lock path
                                    self.uid = uuid.uuid4().hex
                                    self.lockfilename = "%s~%s~%s~%s" % (
                                        self.repositoryType,
                                        self.filename,
                                        self.mode,
                                        self.uid,
                                    )
                                    lockfilepath = os.path.join(
                                        self.lockdir, self.lockfilename
                                    )
                                    # create file
                                    if not os.path.exists(lockfilepath):
                                        # do not make async or use await
                                        with open(
                                            lockfilepath, "w", encoding="UTF-8"
                                        ) as w:
                                            w.write("%d\n" % self.proc)
                                            w.write("%s\n" % self.hostname)
                                            w.write("%s\n" % self.start_time)
                                    logging.debug(
                                        "created transitory lock file %s", lockfilepath
                                    )
                                    # wait
                                    break  # from for loop
                            # found transitory lock, might not wait
                            elif that_mode == self.transitory_lock_mode:
                                # I have transitory lock?
                                if self.mode == self.transitory_lock_mode:
                                    # if yes, tiebreaker between simultaneous transitory locks
                                    t1 = self.start_time
                                    t2 = Locking.getLockStartTime(thatpath)
                                    if t1 < t2:
                                        # won tiebreaker, might acquire target lock
                                        continue  # for loop
                                    elif t1 == t2:
                                        # very unlikely
                                        # deadlock - neither lock knows who wins
                                        # to avoid deadlock, must delete lock
                                        lockfilepath = os.path.join(
                                            self.lockdir, self.lockfilename
                                        )
                                        if os.path.exists(lockfilepath):
                                            os.unlink(lockfilepath)
                                        raise OSError(
                                            "error - deadlock - both locks have same start time - removing lock for %s"
                                            % lockfilepath
                                        )
                                    else:
                                        # lost tiebreaker, wait on other lock
                                        no_conflicts_found = False
                                        break  # from for loop
                                else:
                                    # if no, acquire transitory lock, then wait
                                    self.mode = self.transitory_lock_mode
                                    self.uid = uuid.uuid4().hex
                                    # make new transitory lock path
                                    self.lockfilename = "%s~%s~%s~%s" % (
                                        self.repositoryType,
                                        self.filename,
                                        self.mode,
                                        self.uid,
                                    )
                                    lockfilepath = os.path.join(
                                        self.lockdir, self.lockfilename
                                    )
                                    # create file
                                    if not os.path.exists(lockfilepath):
                                        # do not make async or use await
                                        with open(
                                            lockfilepath, "w", encoding="UTF-8"
                                        ) as w:
                                            w.write("%d\n" % self.proc)
                                            w.write("%s\n" % self.hostname)
                                            w.write("%s\n" % self.start_time)
                                    logging.debug(
                                        "created transitory lock file %s", lockfilepath
                                    )
                                    # wait
                                    no_conflicts_found = False
                                    break  # from for loop
                            else:
                                raise OSError(
                                    "error - unknown lock type %s" % that_mode
                                )
                # test timeout error
                if self.timeout > 0 and time.time() - self.start_time > self.timeout:
                    raise FileExistsError(
                        "error - lock timed out on %s" % self.filename
                    )
                # exit loop condition
                if no_conflicts_found:
                    # if have transitory lock and won tiebreaker or other lock went away
                    if self.mode == self.transitory_lock_mode:
                        logging.debug("converting transitory lock to target lock")
                        # delete transitory file, replace with target lock file (by renaming)
                        transitory_lock_file_path = os.path.join(
                            self.lockdir,
                            "%s~%s~%s~%s"
                            % (
                                self.repositoryType,
                                self.filename,
                                self.transitory_lock_mode,
                                self.uid,
                            ),
                        )
                        self.mode = self.start_mode
                        self.lockfilename = "%s~%s~%s~%s" % (
                            self.repositoryType,
                            self.filename,
                            self.mode,
                            self.uid,
                        )
                        lockfilepath = os.path.join(self.lockdir, self.lockfilename)
                        if os.path.exists(transitory_lock_file_path):
                            # acquire target lock
                            os.replace(transitory_lock_file_path, lockfilepath)
                            logging.info(
                                "acquired %s lock on %s", self.mode, self.lockfilename
                            )
                        else:
                            raise OSError("error - transitory lock file does not exist")
                        logging.debug("completed conversion")
                    # create file if not exists
                    elif not self.lockfilename or not os.path.exists(
                        os.path.join(self.lockdir, self.lockfilename)
                    ):
                        if self.uid is None:
                            self.uid = uuid.uuid4().hex
                        self.lockfilename = "%s~%s~%s~%s" % (
                            self.repositoryType,
                            self.filename,
                            self.mode,
                            self.uid,
                        )
                        lockfilepath = os.path.join(self.lockdir, self.lockfilename)
                        # acquire target lock
                        # do not make async or use await
                        with open(lockfilepath, "w", encoding="UTF-8") as w:
                            w.write("%d\n" % self.proc)
                            w.write("%s\n" % self.hostname)
                            w.write("%s\n" % self.start_time)
                        logging.info(
                            "acquired %s lock on %s", self.mode, self.lockfilename
                        )
                    if found_nothing and self.second_traversal:
                        # even if found nothing, still have risk of simultaneous request by another user
                        # if didn't find nothing in directory, only way to acquire lock is with t lock that has priority, so second traversal not relevant
                        # after both create lock files, both wait briefly and traverse again
                        await asyncio.sleep(self.wait_before_second_traversal)
                        # on finding each other's lock files, a tiebreaker is performed and one of the lock files is rolled back
                        # behavior for more than two simultaneous lock files should be the same
                        if not self.secondTraversal():
                            # roll back locking transaction
                            lockfilepath = os.path.join(self.lockdir, self.lockfilename)
                            if os.path.exists(lockfilepath):
                                os.unlink(lockfilepath)
                            logging.info("rolled back lock on %s", self.lockfilename)
                            # keep waiting and traversing
                            await asyncio.sleep(self.wait_time)
                            continue
                    # acquire lock
                    break  # from while loop
                # otherwise, wait and traverse again
                await asyncio.sleep(self.wait_time)
        except FileExistsError as exc:
            if self.lockfilename is not None and os.path.exists(
                os.path.join(self.lockdir, self.lockfilename)
            ):
                try:
                    os.unlink(os.path.join(self.lockdir, self.lockfilename))
                except Exception:
                    logging.warning(
                        "error - could not remove lock file %s", self.lockfilename
                    )
            raise FileExistsError("%r" % exc)
        except OSError as exc:
            if self.lockfilename is not None and os.path.exists(
                os.path.join(self.lockdir, self.lockfilename)
            ):
                try:
                    os.unlink(os.path.join(self.lockdir, self.lockfilename))
                except Exception:
                    logging.warning(
                        "error - could not remove lock file %s", self.lockfilename
                    )
            raise OSError("%r" % exc)

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        if bool(self.uselock) is False:
            return
        if self.lockfilename is not None and os.path.exists(
            os.path.join(self.lockdir, self.lockfilename)
        ):
            try:
                # comment out to test locking
                os.unlink(os.path.join(self.lockdir, self.lockfilename))
            except Exception:
                logging.warning(
                    "error - could not remove lock file %s", self.lockfilename
                )
        if exc_type or exc_val or exc_tb:
            logging.warning("errors in exit lock for %s", self.lockfilename)
            raise OSError("errors in exit lock")

    def secondTraversal(self):
        # a non-transitory lock, having just acquired lock, waits a few seconds and then traverses directory again to ensure no new conflicting locks are present
        thislockfilepath = os.path.join(self.lockdir, self.lockfilename)
        pattern = "%s~%s" % (self.repositoryType, self.filename)
        logging.debug("second traversal on %s %s", self.lockdir, pattern)
        # traverse to detect new locks
        for thatlockfilepath in glob.iglob("%s/%s*" % (self.lockdir, pattern)):
            thatlockfilename = os.path.basename(thatlockfilepath)
            logging.debug("filename %s", thatlockfilename)
            if (
                thatlockfilename.startswith(pattern)
                and thatlockfilename != self.lockfilename
            ):
                that_mode = thatlockfilename.split("~")[2]
                if that_mode == self.exclusive_lock_mode:
                    if self.mode == self.shared_lock_mode:
                        # shared lock defers to simultaneous exclusive lock
                        return False
                    elif self.mode == self.exclusive_lock_mode:
                        # tiebreaker between simultaneous exclusive locks
                        thisstarttime = Locking.getLockStartTime(thislockfilepath)
                        thatstarttime = Locking.getLockStartTime(thatlockfilepath)
                        if thisstarttime < thatstarttime:
                            # probably acquire lock
                            continue
                        elif thisstarttime == thatstarttime:
                            # deadlock - remove lock file
                            if os.path.exists(thislockfilepath):
                                os.unlink(thislockfilepath)
                            raise OSError("error - locks have same start times")
                        else:
                            return False
                elif that_mode == self.shared_lock_mode:
                    if self.mode == self.exclusive_lock_mode:
                        # probably acquire lock
                        continue
        # traversed all files and found nothing, so acquire lock
        return True

    def getToken(self, tokname):
        # lock file name = repositoryType~filename~mode~uid
        # lock file contains proc \n hostname \n start time
        lockfilepath = os.path.join(self.lockdir, self.lockfilename)
        if tokname == "proc":
            return Locking.getLockProcess(lockfilepath)
        elif tokname == "hostname":
            return Locking.getLockHostname(lockfilepath)
        elif tokname == "start":
            return Locking.getLockStartTime(lockfilepath)
        tokens = self.lockfilename.split("~")
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
        return self.uid == self.getToken("uid")

    def lockExists(self):
        lockfilepath = os.path.join(self.lockdir, self.lockfilename)
        return os.path.exists(lockfilepath)

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
