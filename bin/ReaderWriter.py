import time
import fcntl
import logging
import os
import sys

logging.basicConfig(level=logging.INFO)

# issue - async not allowed on enter

class ReaderWriter(object):
    def __init__(self, fd, mode):
        self.fd = fd
        if mode not in ["reader", "writer"]:
            raise OSError("error - unknown locking mode %s" % mode)
        self.mode = mode
        # problem - without async, would lock a server process for max_seconds
        self.max_seconds = 10

    def __enter__(self):
        if self.mode is not None:
            count = 0
            while True:
                try:
                    if self.mode == "reader":
                        fcntl.flock(self.fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    elif self.mode == "writer":
                        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    logging.info("acquired %s", self.fd)
                    break
                except:
                    logging.info("attempting to acquire %s", self.fd)
                    time.sleep(1)
                count += 1
                print("count %d" % count)
                if count > self.max_seconds:
                    raise OSError("warning - could not obtain lock on %s", self.fd)
                    self.mode = None
                    break

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            raise OSError("errors in exit lock")
        if self.mode is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
