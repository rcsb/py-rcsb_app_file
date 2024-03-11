"""
file - KvBase.py
author - James Smith 2023
"""


class KvBase:
    """
    template interface for kv required functions
    """

    def __init__(self):
        pass

    # generalize from sql table format to redis hash format

    # bulk table functions

    def getKey(self, key, table):
        # return type varies depending on which table was requested
        raise NotImplementedError("kv base get key not implemented")

    def clearTable(self, table):
        raise NotImplementedError("kv base clear table not implemented")

    # map table functions

    def getMap(self, key):
        raise NotImplementedError("kv base get map not implemented")

    def setMap(self, key, val):
        raise NotImplementedError("kv base set map not implemented")

    def clearMapKey(self, key):
        raise NotImplementedError("kv base clear map key not implemented")

    def clearMapVal(self, val):
        raise NotImplementedError("kv base clear map val not implemented")

    # session table functions

    def getSession(self, key1, key2):
        raise NotImplementedError("kv base get session not implemented")

    def setSession(self, key1, key2, val):
        raise NotImplementedError("kv base set session not implemented")

    def clearSessionKey(self, key):
        raise NotImplementedError("kv base clear session key not implemented")

    def clearSessionVal(self, key1, key2):
        raise NotImplementedError("kv base clear session val not implemented")
