# file - KvSqlite.py
# author - James Smith 2023

import typing
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvConnection import KvConnection
from fastapi.exceptions import HTTPException


class KvSqlite(object):
    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.kV = None
        self.__cP = cP
        self.filePath = self.__cP.get("KV_FILE_PATH")
        self.sessionTable = self.__cP.get("KV_SESSION_TABLE_NAME")
        self.mapTable = self.__cP.get("KV_MAP_TABLE_NAME")
        # create database if not exists
        # create table if not exists
        try:
            self.kV = KvConnection(self.filePath, self.sessionTable, self.mapTable)
        except Exception:
            # table already exists
            pass
        if self.kV is None:
            raise HTTPException(
                status_code=400, detail="error in KvSqlite - no database"
            )

    def convert(self, _d):
        return str(_d)

    def deconvert(self, _s):
        return eval(_s)  # pylint: disable=W0123

    def getSession(self, key, val):
        if not key:
            return None
        table = self.sessionTable
        return self.__getDictionary(key, val, table)

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

    def setSession(self, key, val, vval):
        if not key:
            return None
        table = self.sessionTable
        return self.__setDictionary(key, val, vval, table)

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

    def inc(self, key, val):
        table = self.sessionTable
        return self.__incrementDictionaryValue(key, val, table)

    def __incrementDictionaryValue(self, key, val, table):
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

    def clearSessionVal(self, key, val):
        table = self.sessionTable
        return self.__clearDictionaryVal(key, val, table)

    def __clearDictionaryVal(self, key, val, table):
        _s = self.kV.get(key, table)
        if _s is not None:
            _d = self.deconvert(_s)
            if val in _d:
                del _d[val]
                self.kV.set(key, self.convert(_d), table)
                return True
        return False

    def clearSessionKey(self, key):
        table = self.sessionTable
        return self.__clearDictionaryKey(key, table)

    def __clearDictionaryKey(self, key, table):
        _s = self.kV.get(key, table)
        if _s is not None:
            self.kV.clear(key, table)
            return True
        return False

    # get entire dictionary value rather than a sub-value
    def getKey(self, key, table):
        return self.kV.get(key, table)

    def clearTable(self, table):
        self.kV.clearTable(table)

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

    def clearMapVal(self, val):
        table = self.mapTable
        self.kV.deleteRowWithVal(val, table)
