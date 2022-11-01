from keyvalue_sqlite import KeyValueSqlite
import logging


class KvSqlite(object):
    def __init__(self):
        # fix mount point
        self.KV_SQLITE_PATH = "./kv.sqlite"
        self.KV_SESSIONS_TABLE = "sessions"
        # create database if not exists
        # create table if not exists
        try:
            self.KV = KeyValueSqlite(self.KV_SQLITE_PATH, self.KV_SESSIONS_TABLE)
        except Exception as exc:
            # table already exists
            logging.warning(f'exception in KvSqlite: {type(exc)} {exc}')
        if self.KV is None:
            raise Exception(f'error in KvSqlite - no database')

    def get(self, key):
        return self.KV.get(key)

    def set(self, key, val):
        self.KV.set(key, val)

    def convert(self, d):
        return str(d)

    def deconvert(self, s):
        return eval(s)

    def gget(self, key, val):
        s = self.KV.get(key)
        if s is None:
            self.KV.set(key, self.convert({}))
            s = self.KV.get(key)
        d = self.deconvert(s)
        if val not in d:
            d[val] = 0
            self.KV.set(key, self.convert(d))
            s = self.KV.get(key)
            d = self.deconvert(s)
        return d[val]

    def sset(self, key, val, vval):
        s = self.KV.get(key)
        if s is None:
            self.KV.set(key, self.convert({}))
            s = self.KV.get(key)
        d = self.deconvert(s)
        if val not in d:
            d[val] = 0
            self.KV.set(key, self.convert(d))
            s = self.KV.get(key)
            d = self.deconvert(s)
        d[val] = vval
        self.KV.set(key, self.convert(d))

    def inc(self, key, val):
        s = self.KV.get(key)
        if s is None:
            self.KV.set(key, self.convert({}))
            s = self.KV.get(key)
        d = self.deconvert(s)
        if val not in d:
            d[val] = 0
            self.KV.set(key, self.convert(d))
            s = self.KV.get(key)
            d = self.deconvert(s)
        d[val] += 1
        self.KV.set(key, self.convert(d))

    def remove(self, key):
        self.KV.remove(key, ignore_missing_key=True)

    def rremove(self, key, val):
        s = self.KV.get(key)
        if s is not None:
            d = self.deconvert(s)
            if val in d:
                del d[val]
                self.KV.set(key, self.convert(d))


