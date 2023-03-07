import os

HERE = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = os.path.join(HERE, "config.yml")
os.environ["CONFIG_FILE"] = CONFIG_FILE

def getConfig():
    here = os.path.abspath(os.path.dirname(__file__))
    config_file = os.path.join(HERE, "config.yml")
    return config_file
