# file - KvSqlite.py
# author - James Smith 2023

import typing
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvConnection import KvConnection
from fastapi.exceptions import HTTPException


# map redis-style hash vars to sql queries
# not valid across multiple machines or containers


class KvSqlite(object):
    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.kV = None
        self.__cP = cP
        self.filePath = self.__cP.get("KV_FILE_PATH")
        self.sessionTable = self.__cP.get("KV_SESSION_TABLE_NAME")
        self.mapTable = self.__cP.get("KV_MAP_TABLE_NAME")
        self.lockTable = self.__cP.get("KV_LOCK_TABLE_NAME")
        # create database if not exists
        # create table if not exists
        try:
            self.kV = KvConnection(
                self.filePath, self.sessionTable, self.mapTable, self.lockTable
            )
        except Exception:
            # table already exists
            pass
        if self.kV is None:
            raise HTTPException(
                status_code=400, detail="error in KvSqlite - no database"
            )

    # interconvert sql tables and nested dictionaries

    def convert(self, _d):
        return str(_d)

    def deconvert(self, _s):
        return eval(_s)  # pylint: disable=W0123

    def __getDictionary(self, key, val, table):
        _s = self.kV.get(key, table)
        if _s is None:
            self.kV.set(key, self.convert({}), table)
            _s = self.kV.get(key, table)
        _d = self.deconvert(_s)
        if val not in _d:
            _d[val] = 0
            self.kV.set(key, self.convert(_d), table)
            _s = self.kV.get(key, table)
            _d = self.deconvert(_s)
        try:
            return _d[val]
        except Exception:
            raise HTTPException(
                status_code=400, detail=f"error in KV get for table {table}, {_d}"
            )

    def __setDictionary(self, key, val, vval, table):
        _s = self.kV.get(key, table)
        if _s is None:
            self.kV.set(key, self.convert({}), table)
            _s = self.kV.get(key, table)
        _d = self.deconvert(_s)
        if val not in _d:
            _d[val] = 0
            self.kV.set(key, self.convert(_d), table)
            _s = self.kV.get(key, table)
            _d = self.deconvert(_s)
        _d[val] = vval
        self.kV.set(key, self.convert(_d), table)

    # value for key, or for nested dictionary get entire dictionary value-set rather than a sub-value
    def getKey(self, key, table):
        return self.kV.get(key, table)

    def __clearDictionaryKey(self, key, table):
        _s = self.kV.get(key, table)
        if _s is not None:
            self.kV.clear(key, table)
            return True
        return False

    def __clearDictionaryVal(self, key, val, table):
        _s = self.kV.get(key, table)
        if _s is not None:
            _d = self.deconvert(_s)
            if val in _d:
                del _d[val]
                self.kV.set(key, self.convert(_d), table)
                return True
        return False

    def clearTable(self, table):
        self.kV.clearTable(table)

    # map table functions (key, val)

    def getMap(self, key):
        if not key:
            return None
        table = self.mapTable
        return self.kV.get(key, table)

    def setMap(self, key, val):
        if not key:
            return
        table = self.mapTable
        self.kV.set(key, val, table)

    def clearMapKey(self, key):
        table = self.mapTable
        self.kV.deleteRowWithKey(key, table)

    def clearMapVal(self, val):
        table = self.mapTable
        self.kV.deleteRowWithVal(val, table)

    # sessions table functions (nested dictionary - key1, key2, val)

    def getSession(self, key1, key2):
        if not key1:
            return None
        table = self.sessionTable
        return self.__getDictionary(key1, key2, table)

    def setSession(self, key1, key2, val):
        if not key1:
            return None
        table = self.sessionTable
        return self.__setDictionary(key1, key2, val, table)

    def clearSessionKey(self, key):
        table = self.sessionTable
        return self.__clearDictionaryKey(key, table)

    def clearSessionVal(self, key1, key2):
        table = self.sessionTable
        return self.__clearDictionaryVal(key1, key2, table)

    def inc_session_val(self, key, val):
        table = self.sessionTable
        return self.__incrementSessionValue(key, val, table)

    def __incrementSessionValue(self, key, val, table):
        _s = self.kV.get(key, table)
        if _s is None:
            self.kV.set(key, self.convert({}), table)
            _s = self.kV.get(key, table)
        _d = self.deconvert(_s)
        if val not in _d:
            _d[val] = 0
            self.kV.set(key, self.convert(_d), table)
            _s = self.kV.get(key, table)
            _d = self.deconvert(_s)
        _d[val] += 1
        self.kV.set(key, self.convert(_d), table)

    # locking functions

    def getLockAll(self):
        table = self.lockTable
        result = self.kV.getAll(table)
        if result is not None:
            return result
        return None

    def getLock(self, key, *indices):
        if not key:
            if len(indices) == 1:
                return None
            elif len(indices) == 2:
                return None, None
            else:
                return None
        table = self.lockTable
        lst = self.kV.get(key, table)
        if lst is None:
            if len(indices) == 1:
                return None
            elif len(indices) == 2:
                return None, None
            else:
                return None
        if lst is not None:
            lst = eval(lst)  # pylint: disable=W0123
            vals = []
            for i in indices:
                vals.append(lst[i])
            if len(vals) == 1:
                return vals[0]
            elif len(vals) == 2:
                return vals[0], vals[1]
            else:
                return vals
        return None

    def setLock(self, key, val, index=0, start_val=""):
        table = self.lockTable
        lst = self.kV.get(key, table)
        if lst is None:
            self.kV.set(key, start_val, table)
            lst = self.kV.get(key, table)
        if lst is not None:
            lst = eval(lst)  # pylint: disable=W0123
            lst[index] = val
            return self.kV.set(key, str(lst), table)
        return None

    def incLock(self, key, index=0, start_val=""):
        table = self.lockTable
        return self.__incrementLockValue(key, table, index, start_val)

    def __incrementLockValue(self, key, table, index, start_val=""):
        lst = self.kV.get(key, table)
        if lst is None:
            self.kV.set(key, start_val, table)
            lst = self.kV.get(key, table)
        lst = eval(lst)  # pylint: disable=W0123
        try:
            lst[index] += 1
        except Exception:
            return
        self.kV.set(key, str(lst), table)

    def decLock(self, key, index=0, start_val=""):
        table = self.lockTable
        return self.__decrementLockValue(key, table, index, start_val)

    def __decrementLockValue(self, key, table, index, start_val=""):
        lst = self.kV.get(key, table)
        if lst is None:
            self.kV.set(key, start_val, table)
            lst = self.kV.get(key, table)
        lst = eval(lst)  # pylint: disable=W0123
        try:
            lst[index] -= 1
        except Exception:
            return
        self.kV.set(key, str(lst), table)

    def incIncLock(self, key, index1=0, index2=1, start_val=""):
        table = self.lockTable
        return self.__incrementIncrementLockValue(key, table, index1, index2, start_val)

    def __incrementIncrementLockValue(self, key, table, index1, index2, start_val=""):
        lst = self.kV.get(key, table)
        if lst is None:
            self.kV.set(key, start_val, table)
            lst = self.kV.get(key, table)
        lst = eval(lst)  # pylint: disable=W0123
        try:
            lst[index1] += 1
            lst[index2] += 1
        except Exception:
            return
        self.kV.set(key, str(lst), table)

    def incDecLock(self, key, index1=0, index2=1, start_val=""):
        table = self.lockTable
        return self.__incrementIncrementLockValue(key, table, index1, index2, start_val)

    def __incrementDecrementLockValue(self, key, table, index1, index2, start_val=""):
        lst = self.kV.get(key, table)
        if lst is None:
            self.kV.set(key, start_val, table)
            lst = self.kV.get(key, table)
        lst = eval(lst)  # pylint: disable=W0123
        try:
            lst[index1] += 1
            lst[index2] -= 1
        except Exception:
            return
        self.kV.set(key, str(lst), table)

    def decDecLock(self, key, index1=0, index2=1, start_val=""):
        table = self.lockTable
        return self.__decrementDecrementLockValue(key, table, index1, index2, start_val)

    def __decrementDecrementLockValue(self, key, table, index1, index2, start_val=""):
        lst = self.kV.get(key, table)
        if lst is None:
            self.kV.set(key, start_val, table)
            lst = self.kV.get(key, table)
        lst = eval(lst)  # pylint: disable=W0123
        try:
            lst[index1] -= 1
            lst[index2] -= 1
        except Exception:
            return
        self.kV.set(key, str(lst), table)

    def decIncLock(self, key, index1=0, index2=1, start_val=""):
        table = self.lockTable
        return self.__decrementIncrementLockValue(key, table, index1, index2, start_val)

    def __decrementIncrementLockValue(self, key, table, index1, index2, start_val=""):
        lst = self.kV.get(key, table)
        if lst is None:
            self.kV.set(key, start_val, table)
            lst = self.kV.get(key, table)
        lst = eval(lst)  # pylint: disable=W0123
        try:
            lst[index1] -= 1
            lst[index2] += 1
        except Exception:
            return
        self.kV.set(key, str(lst), table)

    def remLock(self, key):
        table = self.lockTable
        return self.kV.clear(key, table)
