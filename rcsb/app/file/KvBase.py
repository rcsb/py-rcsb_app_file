"""
file - KvBase.py
author - James Smith 2023
"""

import typing
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvRedis import KvRedis
from rcsb.app.file.KvSqlite import KvSqlite


class KvBase:
    def __init__(self, cP: typing.Type[ConfigProvider] = None):
        self.kV = None
        self.__cP = cP if cP else ConfigProvider()
        self.sessionTable = self.__cP.get("KV_SESSION_TABLE_NAME")
        self.mapTable = self.__cP.get("KV_MAP_TABLE_NAME")
        self.lockTable = self.__cP.get("KV_LOCK_TABLE_NAME")
        KV_MODE = self.__cP.get("KV_MODE")
        if KV_MODE == "sqlite":
            self.kV = KvSqlite(self.__cP)
        elif KV_MODE == "redis":
            self.kV = KvRedis(self.__cP)
        else:
            logging.exception("error - unknown kv mode %s", KV_MODE)
            return None

    # generalize from sql table format to redis hash format

    # bulk table functions

    def getKey(self, key, table):
        # return type varies depending on which table was requested
        return self.kV.getKey(key, table)

    def clearTable(self, table):
        return self.kV.clearTable(table)

    # map table functions

    def getMap(self, key):
        return self.kV.getMap(key)

    def setMap(self, key, val):
        return self.kV.setMap(key, val)

    def clearMapKey(self, key):
        return self.kV.clearMapKey(key)

    def clearMapVal(self, val):
        return self.kV.clearMapVal(val)

    # session table functions

    def getSession(self, key1, key2):
        return self.kV.getSession(key1, key2)

    def setSession(self, key1, key2, val):
        return self.kV.setSession(key1, key2, val)

    def clearSessionKey(self, key):
        return self.kV.clearSessionKey(key)

    def clearSessionVal(self, key1, key2):
        return self.kV.clearSessionVal(key1, key2)

    # locking functions (redis lock only, though works with sqlite for testing purposes)

    def getLockAll(self):
        return self.kV.getLockAll()

    def getLock(self, key, *indices):
        return self.kV.getLock(key, *indices)

    def setLock(self, key, val, index=0, start_val=""):
        return self.kV.setLock(key, val, index, start_val)

    def incLock(self, key, index=0, start_val=""):
        return self.kV.incLock(key, index, start_val)

    def decLock(self, key, index=0, start_val=""):
        return self.kV.decLock(key, index, start_val)

    def incIncLock(self, key, index1=0, index2=1, start_val=""):
        return self.kV.incIncLock(key, index1, index2, start_val)

    def incDecLock(self, key, index1=0, index2=1, start_val=""):
        return self.kV.incDecLock(key, index1, index2, start_val)

    def decDecLock(self, key, index1=0, index2=1, start_val=""):
        return self.kV.decDecLock(key, index1, index2, start_val)

    def decIncLock(self, key, index1=0, index2=1, start_val=""):
        return self.kV.decIncLock(key, index1, index2, start_val)

    def incIncIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.incIncIfZero(key, uid, index1, index2, index3, start_val)

    def incDecIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.incDecIfZero(key, uid, index1, index2, index3, start_val)

    def decIncIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.decIncIfZero(key, uid, index1, index2, index3, start_val)

    def decDecIfZero(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.decDecIfZero(key, uid, index1, index2, index3, start_val)

    def incIncIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.incIncIfNonNeg(key, uid, index1, index2, index3, start_val)

    def incDecIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.incDecIfNonNeg(key, uid, index1, index2, index3, start_val)

    def decIncIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.decIncIfNonNeg(key, uid, index1, index2, index3, start_val)

    def decDecIfNonNeg(self, key, uid, index1=0, index2=0, index3=0, start_val=""):
        return self.kV.decDecIfNonNeg(key, uid, index1, index2, index3, start_val)

    def remLock(self, key):
        return self.kV.remLock(key)

    def remIfSafe(self, key, uid, index1=0, index2=0, index3=0):
        return self.kV.remIfSafe(key, uid, index1, index2, index3)
