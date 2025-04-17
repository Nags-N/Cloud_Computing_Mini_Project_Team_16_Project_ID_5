package main

import (
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type Store struct {
	raft *raft.Raft
	fsm  *FSM
}

func NewStore(id, raftAddr string, bootstrap bool) *Store {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)

	dataDir := filepath.Join("data", id)
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	if err != nil {
		log.Fatalf("failed to create log store: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.bolt"))
	if err != nil {
		log.Fatalf("failed to create stable store: %v", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 1, os.Stderr)
	if err != nil {
		log.Fatalf("failed to create snapshot store: %v", err)
	}

	// Create TCP listener for Raft communication
	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		log.Fatalf("failed to resolve TCP address: %v", err)
	}
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("failed to create TCP transport: %v", err)
	}

	fsm := &FSM{store: make(map[string]string)}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("failed to create raft: %v", err)
	}

	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			log.Fatalf("failed to bootstrap cluster: %v", err)
		}
	}

	return &Store{raft: r, fsm: fsm}
}

