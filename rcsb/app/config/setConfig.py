# file - setConfig.py
# author - James Smith 2023

import os

def getConfig():
    dirPath = os.path.abspath(os.path.dirname(__file__))
    config_file = os.path.join(dirPath, "config.yml")
    return config_file
