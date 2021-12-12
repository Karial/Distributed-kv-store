package src

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"strconv"
)

type Coordinator struct {
	replicas []string
	guid     string
	guidNum  int
}

func (coor *Coordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.ContentLength, r.Header["Range"])
	key := []byte(r.URL.Path)

	switch r.Method {
	case "GET":
		value := coor.Get(Key(key))
		if value == nil {
			w.Header().Set("Status", "No such key")
			w.WriteHeader(404)
			log.Println(fmt.Sprintf("No such key: %s.", key))
			return
		}

		w.Header().Set("Status", "Ok")
		w.WriteHeader(302)
		err := WriteAll(w, bytes.NewReader([]byte(*value)))
		if err != nil {
			w.Header().Set("Status", "Cannot send value back.")
			w.WriteHeader(404)
			log.Println("Cannot write response.")
			return
		}
	case "PUT":
		value, err := io.ReadAll(r.Body)
		if err != nil {
			w.Header().Set("Status", "Cannot read value from request body")
			w.WriteHeader(404)
			return
		}

		coor.Set(Key(key), Value(value))
		w.WriteHeader(201)
	case "DELETE":
		// unimplemented
		//status := a.Delete(key)
		//w.WriteHeader(status)
	}
}

func RunCoordinator(replicas []string, guid string, port int) {
	coordinator := Coordinator{replicas, guid, 0}
	http.ListenAndServe(fmt.Sprintf(":%d", port), &coordinator)
}

func (coor *Coordinator) GenerateGuid() string {
	result := coor.guid + strconv.FormatInt(int64(coor.guidNum), 10)
	coor.guidNum++
	return result
}

func (coor *Coordinator) Majority() int {
	return len(coor.replicas)/2 + 1
}

func (coor *Coordinator) ChooseWriteTimestamp() WriteTimestamp {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callsDone := make([]chan interface{}, len(coor.replicas))
	for i, replica := range coor.replicas {
		i := i
		callsDone[i] = make(chan interface{})

		replicaClient, err := rpc.DialHTTP("tcp", replica)
		if err != nil {
			log.Printf("Replica rpc connect: %s\n", err.Error())
			continue
		}

		var replicaTimestamp WriteTimestamp
		var args interface{}
		call := replicaClient.Go("Replica.GetMostRecentTimestamp", &args, &replicaTimestamp, nil)
		go func() {
			select {
			case <-call.Done:
				if call.Error != nil {
					log.Println("Replica.GetMostRecentTimestamp failed: ", call.Error)
				}

				callsDone[i] <- replicaTimestamp
			case <-ctx.Done():
			}
		}()
	}

	timestamps, timestamp := Await(callsDone, coor.Majority()), WriteTimestamp(0)
	for _, replicaTimestamp := range timestamps {
		timestamp = MaxTimestamp(timestamp, replicaTimestamp.(WriteTimestamp))
	}
	log.Printf("ChooseWriteTimestamp quorum awaited. Result Timestamp: %d\n", timestamp)

	return timestamp + 1
}

func (coor *Coordinator) Set(key Key, value Value) {
	timestamp := coor.ChooseWriteTimestamp()
	coor.SetWithTimestamp(key, StampedValue{value, timestamp, coor.GenerateGuid()})
}

func (coor *Coordinator) SetWithTimestamp(key Key, value StampedValue) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callsDone := make([]chan interface{}, len(coor.replicas))
	for i, replica := range coor.replicas {
		i := i
		callsDone[i] = make(chan interface{})

		replicaClient, err := rpc.DialHTTP("tcp", replica)
		if err != nil {
			log.Printf("Replica rpc call failed: %s\n", err.Error())
			continue
		}

		localWriteArgs := LocalWriteArgs{key, value}
		var reply interface{}
		call := replicaClient.Go("Replica.LocalWrite", &localWriteArgs, &reply, nil)
		go func() {
			select {
			case <-call.Done:
				if call.Error != nil {
					log.Printf("Replica.LocalWrite failed: ", call.Error)
				}

				callsDone[i] <- struct{}{}
			case <-ctx.Done():
			}
		}()
	}

	Await(callsDone, coor.Majority())
	log.Printf("SetWithTimestamp quorum awaited.\n")

}

func (coor *Coordinator) Get(key Key) *Value {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callsDone := make([]chan interface{}, len(coor.replicas))
	for i, replica := range coor.replicas {
		i := i
		callsDone[i] = make(chan interface{})

		replicaClient, err := rpc.DialHTTP("tcp", replica)
		if err != nil {
			log.Printf("Replica rpc call failed: %s\n", err.Error())
		}

		var stampedValue StampedValue
		call := replicaClient.Go("Replica.LocalRead", &key, &stampedValue, nil)
		go func() {
			select {
			case <-call.Done:
				if call.Error != nil {
					log.Printf("Replica.LocalRead failed: %s\n", call.Error.Error())
				}

				callsDone[i] <- stampedValue
			case <-ctx.Done():
			}
		}()
	}

	stampedValues, stampedValue := Await(callsDone, coor.Majority()), StampedValue{}
	for _, replicaStampedValue := range stampedValues {
		stampedValue = MaxStampedValue(stampedValue, replicaStampedValue.(StampedValue))
	}
	log.Printf("Get quorum awaited. Result Value: %d\n", stampedValue.Value)

	if stampedValue.Guid != "" {
		return &stampedValue.Value
	} else {
		return nil
	}
}
