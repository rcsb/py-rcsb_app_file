# RCSB File Access API Application

## A FastAPI File Access Service Application

[![Build Status](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_app_file?branchName=master)](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=12&branchName=master)

### Installation

Download the library source software from the project repository:

```

git clone --recurse-submodules https://github.com/rcsb/py-rcsb_app_file.git

```

Optionally, run test suite (Python 3.9) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```
python setup.py test

or simply run

tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).

```
pip install rcsb.app.file

or from the local repository directory:

pip install .
```

# Deployment on Local Server

### Testing without docker

From base repository directory (in `py-rcsb_app_file/`), start app with:
```bash

./deploy/LAUNCH_GUNICORN.sh

```

Then, in a separate shell (also from the base repository directory), run individual tests, e.g.:
```bash

python3 ./rcsb/app/tests-file/testClientUtils.py

```


### Build Docker Container

In directory that contains `Dockerfile.stage`:
```

docker build --build-arg USER_ID=<user_id> --build-arg GROUP_ID=<group_id> -t fileapp -f Dockerfile.stage .

```

### Run docker container

```

docker run --rm --name fileapp -p 80:8000 fileapp

```

`-d` runs container in the background, allowing user to 

`–-rm` removes the container after it is stopped

`–-name` allows user to choose a name for the container

`-p` allows user to choose a port, 80:8000 is used in this case, as the port 8000 is exposed in the current dockerfile

# Test upload and download using testClientScript.py

Edit url variables to match server url in testClientScript.py
```

python3 testClientScript.py

```
