package src

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
)

type Key string
type Value string

type WriteTimestamp int

func MaxTimestamp(lhs, rhs WriteTimestamp) WriteTimestamp {
	if lhs > rhs {
		return lhs
	}

	return rhs
}

type StampedValue struct {
	Value     Value
	Timestamp WriteTimestamp
	Guid      string
}

func toBytes(value StampedValue) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	_ = encoder.Encode(value)

	return buf.Bytes()
}

func fromBytes(buf []byte) StampedValue {
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	var value StampedValue
	_ = decoder.Decode(&value)

	return value
}

func MaxStampedValue(lhs, rhs StampedValue) StampedValue {
	if lhs.Value > rhs.Value {
		return lhs
	}

	return rhs
}

func (v *StampedValue) Less(other StampedValue) bool {
	if v.Timestamp != other.Timestamp {
		return v.Timestamp < other.Timestamp
	}

	return v.Guid < other.Guid
}

func WriteAll(dst io.Writer, src io.Reader) error {
	written := 0
	buf := make([]byte, 1024)
	for {
		readed, err := src.Read(buf)
		written += readed
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		needToWrite := written + readed
		for written < needToWrite {
			currentWritten, err := dst.Write(buf[:readed])
			written += currentWritten
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func Await(calls []chan interface{}, num int) []interface{} {
	callResults := make([]interface{}, 0)
	majorityReturned := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	for _, call := range calls {
		call := call
		go func() {
			select {
			case callResult := <-call:
				callResults = append(callResults, callResult)
				if len(callResults) >= num {
					majorityReturned <- struct{}{}
				}
			case <-ctx.Done():
			}
		}()
	}

	<-majorityReturned
	return callResults
}
