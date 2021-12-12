package src

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

type Replica struct {
	db                  *leveldb.DB
	mostRecentTimestamp WriteTimestamp
	mu                  sync.Mutex
}

func RunReplica(port int, dbPath string) {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		panic(fmt.Sprintf("LevelDB open failed: %s", err))
	}
	defer db.Close()

	replica := Replica{db, 0, sync.Mutex{}}
	rpc.Register(&replica)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalln("Listen error: ", err)
	}
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatalln("Cannot start coordinator: ", err)
	}
}

type LocalWriteArgs struct {
	Key   Key
	Value StampedValue
}

func (replica *Replica) Update(key Key, value StampedValue) {
	replica.mostRecentTimestamp = MaxTimestamp(replica.mostRecentTimestamp, value.Timestamp)
	_ = replica.db.Put([]byte(key), toBytes(value), nil)
}

func (replica *Replica) LocalWrite(args *LocalWriteArgs, reply *interface{}) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()
	if buf, err := replica.db.Get([]byte(args.Key), nil); err == nil {
		value := fromBytes(buf)
		if value.Less(args.Value) {
			replica.Update(args.Key, args.Value)
		}
	} else {
		replica.Update(args.Key, args.Value)
	}

	return nil
}

func (replica *Replica) LocalRead(args *Key, reply *StampedValue) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()
	if buf, err := replica.db.Get([]byte(*args), nil); err == nil {
		*reply = fromBytes(buf)
	} else {
		*reply = StampedValue{"", 0, ""}
	}

	return nil
}

func (replica *Replica) GetMostRecentTimestamp(args *interface{}, reply *WriteTimestamp) error {
	*reply = replica.mostRecentTimestamp

	return nil
}
