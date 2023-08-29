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

The only file to configure should be config.yml, with the exception of the example HTML files.

Edit variables in rcsb/app/config/config.yml.

In particular, edit paths (REPOSITORY_DIR_PATH, SHARED_LOCK_PATH, SESSION_DIR_PATH).

For production, they should point to a remote path on a mounted file system.

For testing, they may point to local paths.

Also edit SERVER_HOST_AND_PORT.

Determine the appropriate settings for the server.

Please note that the client will require a different address than the server, so config.yml will require different settings on each.

For example, client - 100.200.300.400:8000, server - 0.0.0.0:8000.

Please note that a proxy server such as nginx may not work from the browser due to a conflict with the CORS middleware in main.py.

The example HTML files (example-upload.html, example-download.html, and example-list.html) must be configured independently.

The relevant variables should be at the top of the files.

In particular, edit url variables to match server url.

# Endpoints and forwarding

To view documentation, run a server, then visit localhost:port_number/docs.

The repository has one upload endpoint, one download endpoint, and one list-directory endpoint, among others.

To upload a file in chunks, use the '/upload' endpoint.

To upload the entire file in one request, set the chunk size parameter equal to the file size.

Upload requires some setup by invoking the '/getUploadParameters' endpoint first, then passing the results as parameters.

To maintain sequential order, the client must wait for each response before sending the next chunk.

The repository saves chunks to a temporary file that is named after the upload id and begins with "._" which is configurable from the getTempFilePath function in Sessions.py.

The download endpoint is found at '/download'.

The list directory endpoint is found at '/list-dir'.

To skip endpoints and forward a server-side chunk or file from Python, use functions in various Utility or Provider files.

Those functions may throw a fastapi.HTTPException, so you will have to enclose function calls in a try except block.

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

Compression of file

- Should hashing be performed before or after compression/decompression? The API performs hashing on the compressed file.
- For the Python client, from the client side, the API first compresses, then hashes the complete file, then uploads. From the server side, the API saves, then hashes the complete file, then decompresses.
- From javascript, hashing libraries are less reliable, so hashing is optional. If a hash digest is not sent as a parameter, the API defaults to file size comparison.
- File size is computed on the compressed file, same as the hash. Please ensure that front-end scripts compute file size in the correct order if compression is used.

Compression of chunks

- When uploading, if the extractChunks parameter is set to True, the API assumes that you have compressed each chunk.
- It therefore decompresses each chunk on receiving it.
- The compression type is set in rcsb/app/config/config.yml.
- Client-side compression is presently only available from the example Python clients.
- The example HTML files do not have compression since compression frameworks from the browser are less developed.
- If you add compression from the browser for compression of chunks, ensure that the compression type matches that specified in config.yml.
- Hashing results are not affected by compression/decompression of chunks.

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

Sqlite cannot be used on a distributed system, therefore it cannot be used in production unless there is only one server and the sqlite database is stored on that server.

Otherwise, to synchronize transactions on multiple servers or containers requires a remote Redis server (do not connect to Redis with 'localhost').

For production, we presume that the file system is a mounted file system.

A Docker container should be used.

To enable scale-up to multiple containers, the file system and database should be installed outside the container.

Set paths in config.yml so that all containers coordinate through the same paths.

Note that Docker requires parameters to bind the paths (refer to examples).

If both Docker and Redis are used, it runs best when Redis is also in a (separate) Docker container (refer to examples).

Some sites could use multiple deposition servers, a situation comparable to multiple containers.

As with containers, multiple servers will require all servers to coordinate through a single remote file system and database.

A proxy server such as nginx may not be compatible with HTML uploads due to CORS policy, though Python should work.

Adding CORS headers to the nginx config file creates conflicts with the CORS middleware in main.py.

Removing the CORS middleware does not seem to be an option either.

As stated previously, please find an appropriate gunicorn server configuration rather than using a proxy server.

# Deployment on local server without docker

For launching without docker, edit url in deploy/LAUNCH_GUNICORN.sh

From base repository directory (in `py-rcsb_app_file/`), start app with:
```bash

./deploy/LAUNCH_GUNICORN.sh

or 

nohup ./deploy/LAUNCH_GUNICORN.sh > /dev/null 2>&1 &

```

# Database

When uploading resumable chunks, server processes coordinate through a database named KV (key-value)

The value of KV_MODE in config.yml determines whether the database is Redis or Sqlite3.

# Sqlite3

Sqlite is provided just for testing.

As configured, Sqlite will only work when the app runs on a single server and the file system and database are also stored on that server.

If KV_MODE is set to sqlite in rcsb/app/config/config.yml, chunk information is coordinated with a sqlite3 database

### To view or remove Sqlite variables

Find KV_FILE_PATH in rcsb/app/config/config.yml

Connect to sqlite and use SQL commands, then ctrl-d to exit
```

sqlite3 path/to/kv.sqlite
.table
select * from sessions;
select * from map;

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
GET keyname
HGETALL hashkey
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

# port number should match port in config.yml

docker run --name fileapp -p 80:80 fileapp

or, if also running a Redis container on the same machine

docker run --name fileapp -p 80:80 --link redis-container:redis fileapp

or, if mounting folders, change paths in rcsb/app/config/config.yml (SESSION_DIR_PATH, REPOSITORY_DIR_PATH, SHARED_LOCK_PATH), enable full permissions for target folder, then

docker run --mount type=bind,source=/path/to/file/system,target=/path/to/file/system --name fileapp -p 80:80 fileapp

or, if also linking to redis container running on same server

docker run --mount type=bind,source=/path/to/file/system,target=/path/to/file/system --name fileapp -p 80:80 --link redis-container:redis fileapp

(observe that the link attribute is not necessary for connecting to Redis running in a container on a different server)

```

`-d` runs container in the background (for production)

`–-rm` removes the container after it is stopped (only for development testing)

`–-name` allows user to choose a name for the container

`-p` allows user to choose a port, which should match the port in config.yml

`--link` connects to a Redis container if the container is running on the same machine as the files API 

# Error handling

For production, a cron job should be run periodically to remove lingering hidden files from the deposit or archive directories and remove Redis or Sqlite sessions.

An example cron script is in the deploy folder.

After development testing with a Sqlite database, open the kv.sqlite file and delete the tables, and delete hidden files from the deposit or archives directories.

After development testing with Redis, open the redis-cli and delete the variables, and delete hidden files from the deposit or archives directories.