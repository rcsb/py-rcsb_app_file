# RCSB File Access API Application

## A FastAPI File Access Service Application

[//]: # ([![Build Status]&#40;https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_app_file?branchName=master&#41;]&#40;https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=12&branchName=master&#41;)

### Installation

Installation is via the program [pip](https://pypi.python.org/pypi/pip).

```
pip3 install rcsb.app.file

```

Or, download the library source software from the project repository:

```
git clone https://github.com/rcsb/py-rcsb_app_file.git

```

then install from the local repository directory:

```
pip3 install .

```

# Configuration 

Edit variables in rcsb/app/config/config.yml.

In particular, edit paths (REPOSITORY_DIR_PATH, SHARED_LOCK_PATH, PDBX_REPOSITORY, SESSION_DIR_PATH, ).

Also edit SERVER_HOST_AND_PORT.

Edit url variables to match server url in example-upload.html, example-download.html, and example-list.html.

# Endpoints and forwarding

To view documentation, run a server, then visit localhost:8000/docs.

The repository has one upload endpoint, one download endpoint, and one list-directory endpoint, among others.

To upload a file in chunks, use the '/upload' endpoint.

To upload the entire file in one request, set the chunk size parameter equal to the file size.

Upload requires some setup by invoking the '/getUploadParameters' endpoint first, then passing the results as parameters.

To maintain sequential order, the client must wait for each response before sending the next chunk.

The repository saves chunks to a temporary file that is named after the upload id and begins with "._" which is configurable from the getTempFilePath function in IoUtility.

The download endpoint is found at '/download'.

The list directory endpoint is found at '/list-dir'.

To skip endpoints and forward a server-side chunk or file from Python, use functions by the same names in various Utility or Provider files.

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
[--download target_folder repo_type id content_type milestone part format version]
[--list repo_type dep_id (list directory)]
[-r (chunk file resumably)]
[-o (overwrite files with same name)]
[-z (zip files prior to upload)]
[-x (expand files after upload)]

```

### Hashing and compression

Should hashing be performed before or after compression/decompression? From the client side, the API first compresses, then hashes the complete file, then uploads. From the server side, the API saves, then hashes the complete file, then decompresses.

# Testing

If you don't have Redis on your local machine, testing is still possible with a Sqlite database.

Run test suite (Python 3.9) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```
python3 setup.py test

or simply run

tox
```

# Deployment

For production, use a Docker container with a Redis database.

Redis with Docker requires Redis in a Docker container.

Production with multiple servers will require all servers to coordinate through a single remote Redis server.

Since one server could host Redis while others don't, the docker instances could be run differently, or the config files set differently, on each server.

Also, multiple servers must connect to a single mounted file system for deposition.

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

Change Redis host to 'localhost' in rcsb/app/config/config.yml, then save.
```

self.kV = redis.Redis(host='localhost', decode_responses=True)

```

Then, from the base directory, reinstall with pip3
```

pip3 install .

```

### Connecting to Redis remotely

If Redis runs on a different machine than the files API, then the host must be set to a url

Change Redis host to '#:#:#:#' and port 6379 in rcsb/app/config/config.yml.

KvRedis.py should resemble

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

If the file access API is running in Docker, then Redis must also run in Docker.

Redis is run from a separate Docker container.

Download Redis image and start container

```
docker run --name redis-container -d redis
or (if connecting remotely to Redis container on different server)
docker run --name redis-container -p 6379:6379 -d redis
```

If the Redis container runs on the same machine as the files API, change Redis host to 'redis' in rcsb/app/config/config.yml.

KvRedis.py should resemble

```

self.kV = redis.Redis(host='redis', port=6379, decode_responses=True)

```

Or, if connecting remotely to Redis container on different server, change Redis host to '#:#:#:#' and port 6379 in rcsb/app/config/config.yml.

KvRedis.py should resemble

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

docker build -t fileapp -f Dockerfile.stage .

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

For production, Redis variables are set to expire periodically. However, hidden files are not, so a cron job should be run periodically to remove lingering hidden files from the deposit or archive directories.

After development testing with a Sqlite database, open the kv.sqlite file and delete the tables, and delete hidden files from the deposit or archives directories.

After development testing with Redis, open the redis-cli and delete the variables, and delete hidden files from the deposit or archives directories.

The hidden files to be deleted are those that start with the value configured in getTempFilePath, referred to above, after checking that the file modification time is beyond a specified threshold.
