from keyvalue_sqlite import KeyValueSqlite


class KvSqlite(object):
    def __init__(self):
        # determine better path
        self.KV_SQLITE_PATH = "./kv.sqlite"
        # create database if not exists
        # create table if not exists
        self.KV = KeyValueSqlite(self.KV_SQLITE_PATH, "sessions")

    def get(self, key):
        return self.KV.get(key)

    def set(self, key, val):
        self.KV.set(key, val)

    def gget(self, key, val):
        return self.KV.get(key).get(val)

    def sset(self, key, val, vval):
        self.KV.get(key).set(val, vval)

    def remove(self, key):
        self.KV.remove(key, ignore_missing_key=True)

    def rremove(self, key, val):
        self.KV.get(key).remove(val)

