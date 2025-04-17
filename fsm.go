package main

import (
	"bytes"
	"encoding/gob"
	"github.com/hashicorp/raft"

	"io"
)

type FSM struct {
	store map[string]string
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd map[string]string
	gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&cmd)
	for k, v := range cmd {
		f.store[k] = v
	}
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{store: f.store}, nil
}

func (f *FSM) Restore(r io.ReadCloser) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(&f.store)
}

func (f *FSM) Get(key string) string {
	return f.store[key]
}


type snapshot struct {
	store map[string]string
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(s.store)
	_, err := sink.Write(buf.Bytes())
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

