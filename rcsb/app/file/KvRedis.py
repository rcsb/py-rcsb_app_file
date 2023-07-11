# file - KvRedis.py
# author - James Smith 2023

import redis
import typing
import logging
from fastapi.exceptions import HTTPException
from rcsb.app.file.ConfigProvider import ConfigProvider


class KvRedis(object):
    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.kV = None
        self.__cP = cP
        self.duration = self.__cP.get("KV_MAX_SECONDS")
        self.sessionTable = self.__cP.get("KV_SESSION_TABLE_NAME")
        self.mapTable = self.__cP.get("KV_MAP_TABLE_NAME")
        self.redis_host = self.__cP.get("REDIS_HOST")  # localhost, redis, or url
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
    def getKey(self, hashvar, table):
        if table == self.sessionTable:
            if not self.kV.exists(hashvar):
                return None
            return self.kV.hgetall(hashvar)
        elif table == self.mapTable:
            if not self.kV.exists(hashvar):
                return None
            return str(self.kV.get(hashvar))

    # clear either hash var or everything (redis has no tables other than possibly hash vars)
    def clearTable(self, hashvar=None):
        if hashvar is not None and self.kV.exists(hashvar):
            self.kV.delete(hashvar)
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
    def clearMap(self, key):
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

    # hashvar = uid
    # if no key, sets to zero and returns, even if not a numeric variable
    def getSession(self, hashvar, key):
        # validate args
        if not hashvar or not key:
            return None
        # validate hashvar key, set val to zero by default
        if not self.kV.exists(hashvar) or not self.kV.hexists(hashvar, key):
            self.kV.hset(hashvar, key, 0)
            self.kV.expire(hashvar, self.duration)
        return str(self.kV.hget(hashvar, key))

    # hashvar = uid
    def setSession(self, hashvar, key, val):
        # validate args
        if not hashvar or not key or not val:
            return False
        # (should not be necessary) validate key, set val to zero by default
        # if not self.kV.exists(hashvar) or not self.kV.hexists(hashvar, key):
        #     self.kV.hset(hashvar, key, 0)
        self.kV.hset(hashvar, key, val)
        # optionally set duration
        # self.kV.expire(hashvar, self.duration)
        return True

    def clearSessionKey(self, hashvar):
        # validate args
        if not hashvar:
            return False
        # validate hashvar
        if not self.kV.exists(hashvar):
            return True
        self.kV.delete(hashvar)
        return True

    def clearSessionVal(self, hashvar, key):
        # validate args
        if not hashvar or not key:
            return False
        # validate hashvar
        if not self.kV.exists(hashvar):
            return True
        # validate key
        if not self.kV.hexists(hashvar, key):
            return True
        self.kV.hdel(hashvar, key)
        return True

    # mathematical functions

    # increment session val
    # presumes key has a numeric value
    def inc(self, hashvar, key):
        # validate args
        if not hashvar or not key:
            return False
        # validate key, set val to zero by default
        if not self.kV.exists(hashvar) or not self.kV.hexists(hashvar, key):
            self.kV.hset(hashvar, key, 0)
            self.kV.expire(hashvar, self.duration)
        self.kV.hincrby(hashvar, key, 1)
        return True
