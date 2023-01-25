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

# Configuration 

Edit paths in rcsb/app/config/config.yml (SESSION_DIR_PATH, REPOSITORY_DIR_PATH, SHARED_LOCK_PATH, PDBX_REPOSITORY).

Edit url variables to match server url in client.py, gui.py, example-upload.html, example-download.html, and example-list.html.

Edit url in LAUNCH_GUNICORN.sh if necessary.


# Endpoints and forwarding

The repository contains three upload endpoints, one download endpoint, and one list-directory endpoint.

For uploading a complete file as a stream, use the 'file-v2/upload' endpoint.

To upload a file in chunks, use either the 'file-v2/sequentialUpload' or 'file-v2/resumableUpload' endpoint.

The sequential endpoint has a minimal code footprint but requires some setup by invoking the 'file-v2/getNewUploadId' and 'file-v2/getSaveFilePath' endpoints first, then passing the results as parameters.

To maintain sequential order, the client must wait for each response before sending the next chunk.

The resumable endpoint has server-side resumability support, and also uses sequential chunks, with the same requirements.

Resumability first requires a request to the 'file-v2/getUploadStatus' endpoint prior to the resumableUpload endpoint.

The download endpoint is found at 'file-v1/download'.

The list directory endpoint is found at 'file-v1/list-dir'.

To skip endpoints and forward a chunk or file from Python, use functions by the same names in IoUtils.py.

Examples of forwarding are found in gui.py when FORWARDING = True, and have yet to be implemented in client.py.

# Uploads and downloads

### HTML examples

The example-upload.html, example-download.html, and example-list.html files demonstrate requests to the endpoints from HTML.

### Python client

In a separate shell (also from the base repository directory) run client.py or gui.py

Gui.py is launched from the shell

Client.py usage
```

python3 client.py
[-h (help)]
[--upload source_file repo_type id content_type milestone part format version]
[--download target_file repo_type id content_type milestone part format version]
[--list repo_type dep_id (list directory)]
[-s (chunk file sequentially)]
[-r (chunk file resumably)]
[-o (overwrite files with same name)]
[-z (zip files prior to upload)]
[-x (expand files after upload)]

```

### Hashing and compression

Should hashing be performed before or after compression/decompression? From the client side, the API first compresses, then hashes the complete file, then uploads. From the server side, the API saves, then hashes the complete file, then decompresses.

# Testing and deployment

Testing is easiest without Docker and using a Sqlite database.

For production, use a Docker container with a Redis database.

# Deployment on local server without docker

For launching without docker, edit url in deploy/LAUNCH_GUNICORN.sh

From base repository directory (in `py-rcsb_app_file/`), start app with:
```bash

./deploy/LAUNCH_GUNICORN.sh

```

# Sqlite3

When uploading resumable chunks, server processes coordinate through a database named KV (key-value)

If KV_MODE is set to sqlite in rcsb/app/config/config.yml, chunk information is coordinated with a sqlite3 database

### To view or remove Sqlite variables

Find KV_FILE_PATH in rcsb/app/config/config.yml

Connect to sqlite and use SQL commands, then ctrl-d to exit
```

sqlite3 path/to/kv.sqlite
.table
select * from sessions;
select * from log;

```

However, if files API is running in Docker, sqlite will not save to path specified in config.yml

Instead, to view or remove Sqlite variables, find kv.sqlite with
```
find / -name kv.sqlite
```

# Redis

If KV_MODE is set to redis in rcsb/app/config/config.yml, resumable chunks coordinate through a Redis database

Install Redis
```
apt install redis
apt install redis-server
apt install redis-tools
```

Start the Redis server
```
/usr/bin/redis-server (preferred)
or
service redis start
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

### Redis on same machine as files API and without Redis in Docker

Change Redis host to 'localhost' in rcsb/app/file/KvRedis.py, then save.
```

self.kV = redis.Redis(host='localhost', decode_responses=True)

```

Then, from the base directory, reinstall with pip3
```

pip3 install .

```

### Connecting to Redis remotely

If Redis runs on a different machine than the files API, then the host must be set to a url

Change Redis host to '#:#:#:#' and port 6379 in rcsb/app/file/KvRedis.py

For example
```

self.kV = redis.Redis(host='1.2.3.4', port=6379, decode_responses=True)

```

Remote Redis requires changing the config file settings on the machine with Redis

From root
```
vim /etc/redis/redis.conf
(comment out the 'bind' statement)
(change 'protected-mode' from 'yes' to 'no')
```

Then start Redis and add the config file as a parameter
```
/usr/bin/redis-server /etc/redis/redis.conf
```

### Redis in Docker

Download Redis image and start container

```
docker run --name redis-container -d redis
or (if connecting remotely to Redis container on different server)
docker run --name redis-container -p 6379:6379 -d redis
```

If the Redis container runs on the same machine as the files API, change Redis host to 'redis' in rcsb/app/file/KvRedis.py
```

self.kV = redis.Redis(host='redis', decode_responses=True)

```

Or, if connecting remotely to Redis container on different server, change Redis host to '#:#:#:#' and port 6379 in rcsb/app/file/KvRedis.py

For example
```

self.kV = redis.Redis(host='1.2.3.4', port=6379, decode_responses=True)

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

# Docker

### Build Docker Container

In directory that contains `Dockerfile.stage`:
```

docker build --build-arg USER_ID=<user_id> --build-arg GROUP_ID=<group_id> -t fileapp -f Dockerfile.stage .

```

### Run docker container

```

docker run --name fileapp -p 8000:8000 fileapp

or, if also running a Redis container on the same machine

docker run --name fileapp -p 8000:8000 --link redis-container:redis fileapp

or, if mounting folders, change paths in rcsb/app/config/config.yml (SESSION_DIR_PATH, REPOSITORY_DIR_PATH, SHARED_LOCK_PATH, PDBX_REPOSITORY), enable full permissions for target folder, then

docker run --mount type=bind,source=/path/to/file/system,target=/path/to/file/system --name fileapp -p 8000:8000 fileapp

or, if also linking to redis container running on same server

docker run --mount type=bind,source=/path/to/file/system,target=/path/to/file/system --name fileapp -p 8000:8000 --link redis-container:redis fileapp

(observe that the link attribute is not necessary for connecting to Redis running in a container on a different server)

```

`-d` runs container in the background (for production)

`–-rm` removes the container after it is stopped (only for development testing)

`–-name` allows user to choose a name for the container

`-p` allows user to choose a port, 8000:8000 is used in this case, as the port 8000 is exposed in the current dockerfile

`--link` connects to a Redis container if the container is running on the same machine as the files API 

# Error handling

Errors related to 'shared locks' are generally fixed by deleting the 'shared-locks' directory and, if necessary, restarting.

For production, Redis variables are set to expire periodically. However, hidden files are not, so a cron job should be run periodically to remove lingering hidden files from the deposit or archive directories.

After development testing with a Sqlite database, open the kv.sqlite file and delete the tables, and delete hidden files from the deposit or archives directories.

After development testing with Redis, open the redis-cli and delete the variables, and delete hidden files from the deposit or archives directories.

