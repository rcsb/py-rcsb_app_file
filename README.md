# RCSB File Access API Application

## A FastAPI File Access Service Application

[![Build Status](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_app_file?branchName=master)](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=12&branchName=master)

### Installation

Download the library source software from the project repository:

```

git clone https://github.com/rcsb/py-rcsb_app_file.git

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
pip3 install rcsb.app.file

or from the local repository directory:

pip3 install .
```

# Deployment on Local Server

### Testing without docker

Set KV_MODE in rcsb/app/config/config.yml to either redis or sqlite.

Install Redis
```
apt install redis
apt install redis-server
apt install redis-tools
```

Start the Redis server
```
service redis start
or
/usr/bin/redis-server
```

To test Redis
```
redis-cli
ping
(should respond PONG)
```

To stop Redis
```
redis-cli
shutdown
(or service redis stop, but not if Redis was started with /usr/bin/redis-server)
```

Change Redis host to 'localhost' in rcsb/app/file/KvRedis.py, then save.
```

self.kV = redis.Redis(host='localhost', decode_responses=True)

```

Then, from the base directory, reinstall with pip3
```

pip3 install .

```

From base repository directory (in `py-rcsb_app_file/`), start app with:
```bash

./deploy/LAUNCH_GUNICORN.sh

```

Then, in a separate shell (also from the base repository directory), test with client.py or gui.py

Gui.py is launched from the shell

Client.py usage
```

python3 client.py
[-h (help)]
[--upload source_file repo_type id content_type milestone part format version overwritable]
[--download target_file repo_type id content_type milestone part format version]
[--list repo_type dep_id (list directory)]
[-c (compress before upload)]

```

To view Redis variables
```

redis-cli
KEYS *
exit

```

To remove all variables
```

redis-cli
FLUSHALL
exit

```

To view or remove Sqlite variables

Find KV_FILE_PATH in rcsb/app/config/config.yml

Connect to sqlite and use SQL commands, then ctrl-d to exit
```

sqlite3 path/to/kv.sqlite
.table
select * from sessions;
select * from log;

```

### Testing with Docker

Set KV_MODE in rcsb/app/config/config.yml to either redis or sqlite.

For Redis, download Redis image and start container

```
docker run --name redis-container -d redis
or (if connecting remotely to Redis container on different server)
docker run --name redis-container -p 6379:6379 -d redis
```

Then change Redis host to 'redis' in rcsb/app/file/KvRedis.py
```

self.kV = redis.Redis(host='redis', decode_responses=True)

```

Or, if connecting remotely to Redis container on different server, change Redis host to '#:#:#:#' and port 6379 in rcsb/app/file/KvRedis.py
For example
```

self.kV = redis.Redis(host='1.2.3.4', port=6379, decode_responses=True)

```

### Build Docker Container

In directory that contains `Dockerfile.stage`:
```

docker build --build-arg USER_ID=<user_id> --build-arg GROUP_ID=<group_id> -t fileapp -f Dockerfile.stage .

```

### Run docker container

```

docker run --rm --name fileapp -p 8000:8000 --link redis-container:redis fileapp

or, if mounting folders, change paths in rcsb/app/config/config.yml (SESSION_DIR_PATH, REPOSITORY_DIR_PATH, SHARED_LOCK_PATH, PDBX_REPOSITORY), enable full permissions for target folder, then

docker run --mount type=bind,source=/path/to/file/system,target=/path/to/file/system --name fileapp -p 8000:8000 fileapp

or, if also linking to redis container running on same server

docker run --mount type=bind,source=/path/to/file/system,target=/path/to/file/system --name fileapp -p 8000:8000 --link redis-container:redis fileapp

(observer that the link attribute is not necessary for connecting to Redis running in a container on a different server)

```

`-d` runs container in the background (for production)

`–-rm` removes the container after it is stopped (only for development testing)

`–-name` allows user to choose a name for the container

`-p` allows user to choose a port, 8000:8000 is used in this case, as the port 8000 is exposed in the current dockerfile

`--link` connects to the Redis container that was created previously

Test upload and download using client.py or gui.py

Client.py usage
```

python3 client.py
[-h (help)]
[--upload source_file repo_type id content_type milestone part format version overwritable]
[--download target_file repo_type id content_type milestone part format version]
[--list repo_type dep_id (list directory)]
[-c (compress files before upload)]

```

To view Redis variables
```

docker run -it --name redis-viewer --link redis-container:redis --rm redis redis-cli -h redis -p 6379
KEYS *
exit

```

To remove all variables
```

docker run -it --name redis-viewer --link redis-container:redis --rm redis redis-cli -h redis -p 6379
FLUSHALL
exit

```

To view or remove Sqlite variables

Sqlite will not save to path specified in config.yml

Instead, find kv.sqlite with
```
find / -name kv.sqlite
```

Connect to sqlite and use SQL commands, then ctrl-d to exit
```

sqlite3 path/to/kv.sqlite
.table
select * from sessions;

```

# Deployment on Remote Server

Edit url variables to match server url in client.py or gui.py

Edit paths in rcsb/app/config/config.yml (SESSION_DIR_PATH, REPOSITORY_DIR_PATH, SHARED_LOCK_PATH, PDBX_REPOSITORY)

For launching without docker, edit url in deploy/LAUNCH_GUNICORN.sh

# Connecting to Redis remotely

In addition to the above instructions, remote Redis requires changing the config file settings

From root
```
vim /etc/redis/redis.conf
(comment out the 'bind' statement)
(change 'protected-mode' from 'yes' to 'no')
```

Then start Redis and add the config file
```
/usr/bin/redis-server /etc/redis/redis.conf
```

