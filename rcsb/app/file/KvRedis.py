import redis
import typing
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider


class KvRedis:
    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.kV = None
        self.__cP = cP
        self.duration = self.__cP.get("KV_MAX_SECONDS")
        self.sessionTable = self.__cP.get("KV_SESSION_TABLE_NAME")
        self.logTable = self.__cP.get("KV_LOG_TABLE_NAME")
        # create database if not exists
        # create table if not exists
        try:
            self.kV = redis.Redis(host="localhost", decode_responses=True)
        except Exception as exc:
            # already exists
            logging.warning("exception in KvRedis: %s %s", type(exc), exc)

        if self.kV is None:
            raise Exception("error in KvRedis - no database")

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
        return self.kV.hget(hashvar, key)

    # hashvar = uid
    def setSession(self, hashvar, key, val):
        # validate args
        if not hashvar or not key or not val:
            return False
        # validate key, set val to zero by default
        if not self.kV.exists(hashvar) or not self.kV.hexists(hashvar, key):
            self.kV.hset(hashvar, key, 0)
            self.kV.expire(hashvar, self.duration)
        self.kV.hset(hashvar, key, val)
        return True

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

    def clearSessionKey(self, hashvar):
        # validate args
        if not hashvar:
            return False
        # validate hashvar
        if not self.kV.exists(hashvar):
            return True
        self.kV.delete(hashvar)
        return True

    # get entire dictionary value rather than a sub-value
    def getKey(self, hashvar, table):
        if table == self.sessionTable:
            return self.kV.hgetall(hashvar)
        elif table == self.logTable:
            return self.kV.get(hashvar)

    def getLog(self, key):
        # validate args
        if not key:
            return None
        return self.kV.get(key)

    def setLog(self, key, val):
        # validate args
        if not key:
            return False
        self.kV.set(key, val)
        return True

    def clearLog(self, key):
        self.kV.delete(key)

    # clear everything
    def clearTable(self):
        self.kV.flushall()
