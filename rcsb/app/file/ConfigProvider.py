##
# File:    ConfigProvider.py
# Author:  jdw
# Date:    16-Aug-2021
# Version: 0.001
#
# Updates:
##
"""
Accessors for configuration details.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "john.westbrook@rcsb.org"
__license__ = "Apache 2.0"

import datetime
import logging
import os
import typing

# from mmcif.api.DataCategoryBase import DataCategoryBase
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.SingletonClass import SingletonClass
# from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class ConfigProvider(SingletonClass):
    """Accessors for configuration details."""

    def __init__(self, configFilePath: typing.Optional[str] = None):  # cachePath: typing.Optional[str] = None,
        # ---
        # self.__cachePath = cachePath if cachePath else os.environ.get("CACHE_PATH", os.path.abspath("./CACHE"))
        self.__configFilePath = configFilePath if configFilePath else os.environ.get("CONFIG_FILE")
        self.__configD = None
        # self.__dataObj = None
        # ---

    def get(self, ky: str) -> typing.Optional[str]:
        try:
            if not self.__configD:
                if self.__configFilePath:
                    self.__readConFigFromConFigYmlFile()
                # else:
                #    self.__readConfig()
                #
            return self.__configD["data"][ky]
        except Exception:
            pass
        return None

    def getConfig(self) -> typing.Dict:
        try:
            if not self.__configD:
                if self.__configFilePath:
                    self.__readConFigFromConFigYmlFile()
                # else:
                #    self.__readConfig()
                #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__configD["data"]

    def getVersion(self) -> typing.Optional[str]:
        try:
            return self.__configD["version"]
        except Exception:
            pass
        return None

    def __readConFigFromConFigYmlFile(self) -> bool:
        """Read Yml configuration file and set internal configuration dictionary"""
        ok = False
        try:
            cfgOb = ConfigUtil(configPath=self.__configFilePath, defaultSectionName="configuration", mockTopPath=None)
            self.__configD = {"version": 0.30, "created": datetime.datetime.now().isoformat(), "data": cfgOb.exportConfig(sectionName="configuration")}
            ok = True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            ok = False
        return ok
