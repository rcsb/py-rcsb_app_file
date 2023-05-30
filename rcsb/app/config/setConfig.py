import os

# HERE = os.path.abspath(os.path.dirname(__file__))
# CONFIG_FILE = os.path.join(HERE, "config.yml")
# os.environ["CONFIG_FILE"] = os.environ.get("CONFIG_FILE", CONFIG_FILE)

def getConfig():
    dirPath = os.path.abspath(os.path.dirname(__file__))
    config_file = os.path.join(dirPath, "config.yml")
    return config_file
