# Distributed-kv-store

Simple distributed kv-store using ABD algorithm.

### API

- GET /key
    - Get value by key. 302 = found key.
- PUT /key
    - Put key with value. 201 = written, anything else = probably not written.
- DELETE /key
    - Delete value by key. 204 = deleted, anything else = probably not deleted.

### Start replicas

```
./run.sh --replica --port 3001 --db /tmp/pbd1/ &
./run.sh --replica --port 3002 --db /tmp/pbd2/ &
./run.sh --replica --port 3003 --db /tmp/pbd3/ &
```

### Start Master Server (default port 3000)

```
./run.sh --coordinator --port 3000 --replicas localhost:3001,localhost:3002,localhost:3003 --guid Coordinator1
```


### Usage

```
# put "bigswag" in key "wehave"
curl -L -X PUT -d bigswag localhost:3000/wehave

# get key "wehave" (should be "bigswag")
curl -L localhost:3000/wehave

# delete key "wehave"
curl -L -X DELETE localhost:3000/wehave

# put file in key "file.txt"
curl -v -L -X PUT -T /path/to/local/file.txt localhost:3000/file.txt

# get file in key "file.txt"
curl -v -L -o /path/to/local/file.txt localhost:3000/file.txt
```

### ./run.sh Usage

```
Usage: ./run.sh <coorindator, replica>

  -coordinator
        Do we start coordinator
  -db string
        Path to leveldb
  -guid string
        Global unique identifier for coordinator. Should be unique between all coordinators.
  -port int
        Port for the server to listen on (default 3000)
  -replica
        Do we start replica
  -replicas string
        Replicas to use for storage, comma separated
  -v    Verbose output
```