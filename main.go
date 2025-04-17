package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

var store *Store
var raftNode *raft.Raft

func main() {
	// Get env vars
	nodeID := os.Getenv("NODE_ID")
	httpAddr := os.Getenv("HTTP_ADDR")
	raftAddr := os.Getenv("RAFT_ADDR")
	bootstrap := os.Getenv("BOOTSTRAP") == "true"

	store = NewStore(nodeID, raftAddr, bootstrap)
	raftNode = store.raft

	// Setup HTTP routes
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/join", handleJoin)

	log.Println("HTTP server starting at", httpAddr)
	log.Fatal(http.ListenAndServe(httpAddr, nil))
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	if raftNode.State() != raft.Leader {
		leader := raftNode.Leader()
		if leader == "" {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		http.Redirect(w, r, "http://"+string(leader)+r.RequestURI, http.StatusTemporaryRedirect)
		return
	}

	var req map[string]string
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(req)

	applyFuture := raftNode.Apply(buf.Bytes(), 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, "OK")
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := store.fsm.Get(key)
	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

func handleJoin(w http.ResponseWriter, r *http.Request) {
	if raftNode.State() != raft.Leader {
		http.Error(w, "not leader", http.StatusBadRequest)
		return
	}
	var req struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f := raftNode.AddVoter(raft.ServerID(req.ID), raft.ServerAddress(req.Addr), 0, 0)
	if err := f.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, "joined")
}

