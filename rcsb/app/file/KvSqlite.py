import typing
from rcsb.app.file.ConfigProvider import ConfigProvider
from rcsb.app.file.KvConnection import KvConnection


class KvSqlite:
    def __init__(self, cP: typing.Type[ConfigProvider]):
        self.kV = None
        self.__cP = cP
        # fix mount point
        self.filePath = self.__cP.get("KV_FILE_PATH")
        self.sessionTable = self.__cP.get("KV_SESSION_TABLE_NAME")
        self.logTable = self.__cP.get("KV_LOG_TABLE_NAME")
        # create database if not exists
        # create table if not exists
        try:
            self.kV = KvConnection(self.filePath, self.sessionTable, self.logTable)
        except Exception:
            # table already exists
            pass
        if self.kV is None:
            raise Exception("error in KvSqlite - no database")

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
            raise Exception(f"error in KV get for table {table}, {_d}")

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

    def getLog(self, key):
        if not key:
            return None
        table = self.logTable
        return self.kV.get(key, table)

    def setLog(self, key, val):
        if not key:
            return
        table = self.logTable
        self.kV.set(key, val, table)

    def clearLogVal(self, val):
        table = self.logTable
        self.kV.deleteRowWithVal(val, table)
