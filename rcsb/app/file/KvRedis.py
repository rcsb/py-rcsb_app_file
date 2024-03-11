# file - KvRedis.py
# author - James Smith 2023

import redis
import typing
import logging
from fastapi.exceptions import HTTPException
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvBase import KvBase


class KvRedis(KvBase):
    def __init__(self, cP: typing.Type[ConfigProvider] = None):
        super(KvRedis, self).__init__()
        self.kV = None
        self.cP = cP if cP else ConfigProvider()
        self.sessionTable = self.cP.get("KV_SESSION_TABLE_NAME")
        self.mapTable = self.cP.get("KV_MAP_TABLE_NAME")
        self.lockTable = self.cP.get("KV_LOCK_TABLE_NAME")
        self.redis_host = self.cP.get("REDIS_HOST")  # localhost, redis, or url
        self.duration = self.cP.get("KV_MAX_SECONDS")
        # create database if not exists
        # create table if not exists
        try:
            self.kV = redis.Redis(
                host=self.redis_host, port=6379, decode_responses=True
            )
        except Exception as exc:
            # already exists
            logging.warning("exception in KvRedis: %s %s", type(exc), exc)

        if self.kV is None:
            raise HTTPException(
                status_code=400, detail="error in KvRedis - no database"
            )

    # functions for either table

    # get entire dictionary value rather than a sub-value
    def getKey(self, key, table):
        if table == self.sessionTable:
            if not self.kV.exists(key):
                return None
            return self.kV.hgetall(key)
        if table == self.mapTable:
            if not self.kV.exists(key):
                return None
            return str(self.kV.get(key))
        return None

    # clear either table or everything (redis has no tables other than possibly hash vars)
    def clearTable(self, table=None):
        if table is not None and self.kV.exists(table):
            self.kV.delete(table)
        else:
            self.kV.flushall()

    # map table functions (key, val)

    # get val from key
    def getMap(self, key):
        # validate args
        if not key:
            return None
        if not self.kV.exists(key):
            return None
        return str(self.kV.get(key))

    def setMap(self, key, val):
        # validate args
        if not key:
            return False
        self.kV.set(key, val)
        return True

    # delete key
    def clearMapKey(self, key):
        self.kV.delete(key)

    # delete key from val
    def clearMapVal(self, val):
        key = self.getMapKeyFromVal(val)
        self.kV.delete(key)

    # helper for delete-key-from-val
    def getMapKeyFromVal(self, val):
        if not val:
            return None
        keys = self.kV.keys("*")
        for k in keys:
            if self.kV.exists(k):
                v = self.kV.get(k)
                if not isinstance(v, dict) and v == val:
                    return k
        return None

    # sessions table functions (nested dictionary - key1, key2, val)

    # key1 = uid
    # if no key, sets to zero and returns, even if not a numeric variable
    def getSession(self, key1, key2):
        # validate args
        if not key1 or not key2:
            return None
        # validate keys, set val to zero by default
        if not self.kV.exists(key1) or not self.kV.hexists(key1, key2):
            self.kV.hset(key1, key2, 0)
            self.kV.expire(key1, self.duration)
        return str(self.kV.hget(key1, key2))

    # key1 = uid
    def setSession(self, key1, key2, val):
        # validate args
        if not key1 or not key2 or not val:
            return False
        # (should not be necessary) validate key, set val to zero by default
        # if not self.kV.exists(key1) or not self.kV.hexists(key1, key2):
        #     self.kV.hset(key1, key2, 0)
        self.kV.hset(key1, key2, val)
        # optionally set duration
        # self.kV.expire(key1, self.duration)
        return True

    def clearSessionKey(self, key):
        # validate args
        if not key:
            return False
        # validate key
        if not self.kV.exists(key):
            return True
        self.kV.delete(key)
        return True

    def clearSessionVal(self, key1, key2):
        # validate args
        if not key1 or not key2:
            return False
        # validate key1
        if not self.kV.exists(key1):
            return True
        # validate key2
        if not self.kV.hexists(key1, key2):
            return True
        self.kV.hdel(key1, key2)
        return True

    # increment session val
    # presumes key2 has a numeric value
    def inc_session_val(self, key1, key2):
        # validate args
        if not key1 or not key2:
            return False
        # validate key, set val to zero by default
        if not self.kV.exists(key1) or not self.kV.hexists(key1, key2):
            self.kV.hset(key1, key2, 0)
            self.kV.expire(key1, self.duration)
        self.kV.hincrby(key1, key2, 1)
        return True

    # locking functions

    def getLockAll(self):
        if not self.kV.exists(self.lockTable):
            return None
        return self.kV.hgetall(self.lockTable)

    def getLock(self, key, *indices):
        if not key:
            if len(indices) == 1:
                return None
            if len(indices) == 2:
                return None, None
            return None
        if not self.kV.hexists(self.lockTable, key):
            if len(indices) == 1:
                return None
            if len(indices) == 2:
                return None, None
            return None
        lst = self.kV.hget(self.lockTable, key)
        vals = []
        if lst is not None:
            lst = eval(lst)  # pylint: disable=W0123
            for i in indices:
                vals.append(lst[i])
            if len(vals) == 1:
                return vals[0]
            if len(vals) == 2:
                return vals[0], vals[1]
            return vals
        return None

    def setLock(self, key, val, index=0, start_val=""):
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
        lst[index] = val
        self.kV.hset(self.lockTable, key, str(lst))
        return True

    # increment val
    def incLock(self, key, index=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
        try:
            lst[index] += 1
        except Exception:
            return False
        self.kV.hset(self.lockTable, key, str(lst))
        return True

    # decrement val
    def decLock(self, key, index=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
        try:
            lst[index] -= 1
        except Exception:
            return False
        self.kV.hset(self.lockTable, key, str(lst))
        return True

    # atomic operations

    def incIncLock(self, key, index1=0, index2=1, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            try:
                lst[index1] += 1
                lst[index2] += 1
            except Exception:
                result = False
            else:
                self.kV.hset(self.lockTable, key, str(lst))
        return result

    def incDecLock(self, key, index1=0, index2=1, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            try:
                lst[index1] += 1
                lst[index2] -= 1
            except Exception:
                result = False
            else:
                self.kV.hset(self.lockTable, key, str(lst))
        return result

    def decDecLock(self, key, index1=0, index2=1, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            try:
                lst[index1] -= 1
                lst[index2] -= 1
            except Exception:
                result = False
            else:
                self.kV.hset(self.lockTable, key, str(lst))
        return result

    def decIncLock(self, key, index1=0, index2=1, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            try:
                lst[index1] -= 1
                lst[index2] += 1
            except Exception:
                result = False
            else:
                self.kV.hset(self.lockTable, key, str(lst))
        return result

    def lockHasWaitList(self, key, index) -> bool:
        # find out if lock is waitlisted
        s = str(self.getLock(key, index))
        return bool(s != "-1")

    def getWaitList(self, key, index):
        # return lock waitlist value
        return self.getLock(key, index)

    def reservedWaitList(self, key, index, uid):
        # true if I reserved the lock, false otherwise
        return self.getWaitList(key, index) == uid

    # remove lock variable
    def remLock(self, key):
        if not key:
            return False
        if self.kV.hexists(self.lockTable, key):
            self.kV.hdel(self.lockTable, key)
        return True

    # atomic transactions that have not been implemented in sqlite
    def incIncIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod == 0
                and count == 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] += 1
                    lst[index2] += 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    def incDecIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod == 0
                and count == 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] += 1
                    lst[index2] -= 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    def decIncIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod == 0
                and count == 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] -= 1
                    lst[index2] += 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    def decDecIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod == 0
                and count == 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] -= 1
                    lst[index2] -= 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    def incIncIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod >= 0
                and count >= 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] += 1
                    lst[index2] += 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    def incDecIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod >= 0
                and count >= 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] += 1
                    lst[index2] -= 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    def decIncIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod >= 0
                and count >= 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] -= 1
                    lst[index2] += 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    def decDecIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        # validate args
        if not key:
            return False
        if not self.kV.hexists(self.lockTable, key):
            self.kV.hset(self.lockTable, key, start_val)
        result = True
        mod = None
        count = None
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            mod = lst[index1]
            count = lst[index2]
            if (
                mod >= 0
                and count >= 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                try:
                    lst[index1] -= 1
                    lst[index2] -= 1
                except Exception:
                    result = False
                else:
                    self.kV.hset(self.lockTable, key, str(lst))
            else:
                result = False
        return result, mod, count

    # atomic removal
    def remIfSafe(self, key, uid, index1=0, index2=0, index3=0):
        if not key:
            return False
        with redis.lock.Lock(self.kV, key):
            lst = eval(self.kV.hget(self.lockTable, key))  # pylint: disable=W0123
            if lst is None:
                return False
            mod = lst[index1]
            count = lst[index2]
            waitlist = lst[index3]
            if mod is None or count is None or waitlist is None:
                return False
            if (
                mod == 0
                and count == 0
                and (
                    (not self.lockHasWaitList(key, index3))
                    or self.reservedWaitList(key, index3, uid)
                )
            ):
                self.remLock(key)
        return True
