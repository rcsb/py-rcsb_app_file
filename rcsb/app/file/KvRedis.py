import redis
import typing
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider

class KvRedis:
    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.kV = None
        self.__cP = cP
        self.duration = self.__cP.get('KV_MAX_SECONDS')
        # fix mount point
        # self.filePath = self.__cP.get("KV_FILE_PATH")
        self.sessionTable = self.__cP.get("KV_SESSION_TABLE_NAME")
        self.logTable = self.__cP.get("KV_LOG_TABLE_NAME")
        # create database if not exists
        # create table if not exists
        try:
            self.kV = redis.Redis(host='localhost', decode_responses=True)
        except Exception as exc:
            # already exists
            logging.warning(f'exception in KvRedis: {type(exc)} {exc}')
            pass
        if self.kV is None:
            raise Exception("error in KvRedis - no database")

    # hash = uid
    # if no key, sets to zero and returns, even if not a numeric variable
    def getSession(self, hash, key):
        # validate args
        if not hash or not key:
            return None
        # validate hash
        # if not self.kV.exists(hash):
        #     self.kV.hmset(hash, {})
        # validate hash key, set val to zero by default
        if not self.kV.exists(hash) or not self.kV.hexists(hash, key):
            self.kV.hset(hash, key, 0)
            self.kV.expire(hash, self.duration)
        return self.kV.hget(hash, key)

    # hash = uid
    def setSession(self, hash, key, val):
        # validate args
        if not hash or not key or not val:
            return False
        # validate hash
        # if not self.kV.exists(hash):
        #     self.kV.hmset(hash, {})
        #     self.kV.expire(hash, self.duration)
        # validate key, set val to zero by default
        if not self.kV.exists(hash) or not self.kV.hexists(hash, key):
            self.kV.hset(hash, key, 0)
            self.kV.expire(hash, self.duration)
        self.kV.hset(hash, key, val)
        return True

    # increment session val
    # presumes key has a numeric value
    def inc(self, hash, key):
        # validate args
        if not hash or not key:
            return False
        # validate hash
        # if not self.kV.exists(hash):
        #     self.kV.hmset(hash, {})
        #     self.kV.expire(hash, self.duration)
        # validate key, set val to zero by default
        if not self.kV.exists(hash) or not self.kV.hexists(hash, key):
            self.kV.hset(hash, key, 0)
            self.kV.expire(hash, self.duration)
        self.kV.hincrby(hash, key, 1)
        return True

    def clearSessionVal(self, hash, key):
        # validate args
        if not hash or not key:
            return False
        # validate hash
        if not self.kV.exists(hash):
            return True
        # validate key
        if not self.kV.hexists(hash, key):
            return True
        self.kV.hdel(hash, key)
        return True

    def clearSessionKey(self, hash):
        # validate args
        if not hash:
            return False
        # validate hash
        if not self.kV.exists(hash):
            return True
        self.kV.delete(hash)
        return True

    # get entire dictionary value rather than a sub-value
    def getKey(self, hash, table):
        if table == self.sessionTable:
            return self.kV.hgetall(hash)
        elif table == self.logTable:
            return self.kV.get(hash)

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

    # no tables in Redis, instead clear everything
    def clearTable(self, table=None):
        self.kV.flushall()
        # self.kV.clearTable(table)