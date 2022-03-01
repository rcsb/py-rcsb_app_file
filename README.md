# py-rcsb_app_file

File Access Service Application

### Installation

Download the library source software from the project repository:

```bash

git clone --recurse-submodules https://github.com/rcsb/py-rcsb_file_chem.git

```

Optionally, run test suite (Python 3.9) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```bash
python setup.py test

or simply run

tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).

```bash
pip install rcsb.app.file

or from the local repository directory:

pip install .
```

# Deployment on Local Server

### Build Docker Container

```bash

In directory that contains “Dockerfile.devel”

docker build -t fileapp -f Dockerfile.devel .

```

### Run docker container

```bash

docker run –rm –name fileapp -p 80:8000 fileapp

```

-d runs container in the background, allowing user to 

–rm removes the container after it is stopped

–name allows user to choose a name for the container

-p allows user to choose a port, 80:8000 is used in this case, as the port 8000 is exposed in the current dockerfile
