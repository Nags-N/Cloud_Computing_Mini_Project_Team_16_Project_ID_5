package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/google/uuid"
)

type Node struct {
	id    string
	raft  *raft.Raft
	store *fsm
}

func main() {
	// === CLI flags ===
	nodeID := flag.String("id", "", "Unique ID for this node")
	raftPort := flag.String("raft-port", "12000", "Port for Raft communication")
	httpPort := flag.String("http-port", "8080", "Port for HTTP server")
	joinAddr := flag.String("join", "", "Address to join an existing cluster")

	flag.Parse()

	if *nodeID == "" {
		log.Fatal("You must provide --id for this node.")
	}

	// === Setup dirs ===
	dataDir := fmt.Sprintf("raft-data-%s", *nodeID)
	os.MkdirAll(dataDir, 0700)

	// === Create Raft config ===
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	config.SnapshotInterval = 20 * time.Second
	config.SnapshotThreshold = 1024

	// Log store
	logStore, err := raftboltdb.NewBoltStore(fmt.Sprintf("%s/log.db", dataDir))
	if err != nil {
		log.Fatalf("log store error: %s", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(fmt.Sprintf("%s/stable.db", dataDir))
	if err != nil {
		log.Fatalf("stable store error: %s", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stdout)
	if err != nil {
		log.Fatalf("snapshot store error: %s", err)
	}

	// Transport layer
	addr := fmt.Sprintf("127.0.0.1:%s", *raftPort)
	transport, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, os.Stdout)
	if err != nil {
		log.Fatalf("transport error: %s", err)
	}

	// Initialize FSM with empty store
	node := &Node{
		id: *nodeID,
		store: &fsm{
			store: Store{
				Printers:  make(map[string]Printer),
				Filaments: make(map[string]Filament),
				PrintJobs: make(map[string]PrintJob),
			},
		},
	}

	// Init Raft
	r, err := raft.NewRaft(config, node.store, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("raft init error: %s", err)
	}
	node.raft = r

	// Bootstrap or Join
	if *joinAddr == "" {
		log.Println("[BOOTSTRAP] Starting new single-node cluster...")
		r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{ID: config.LocalID, Address: transport.LocalAddr()},
			},
		})
	} else {
		// Join existing cluster via HTTP
		log.Printf("[JOINING] Attempting to join cluster at %s...\n", *joinAddr)
		joinURL := fmt.Sprintf("http://%s/join?peerID=%s&peerAddr=%s", *joinAddr, *nodeID, addr)
		resp, err := http.Get(joinURL)
		if err != nil {
			log.Fatalf("Join failed: %v", err)
		}
		resp.Body.Close()
		log.Println("Successfully joined cluster.")
	}

	// === HTTP API Handlers ===
	
	// Raft state endpoint
	http.HandleFunc("/raft/state", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]string{
			"state":  node.raft.State().String(),
			"leader": string(node.raft.Leader()),
			"nodeID": node.id,
		}
		json.NewEncoder(w).Encode(status)
	})

	// Raft join endpoint
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		peerID := r.URL.Query().Get("peerID")
		peerAddr := r.URL.Query().Get("peerAddr")
		if peerID == "" || peerAddr == "" {
			http.Error(w, "Missing peerID or peerAddr", http.StatusBadRequest)
			return
		}
		log.Printf("Received join request from %s (%s)", peerID, peerAddr)
		
		// Only leader can add servers
		if node.raft.State() != raft.Leader {
			http.Error(w, "Not the leader", http.StatusForbidden)
			return
		}
		
		f := node.raft.AddVoter(raft.ServerID(peerID), raft.ServerAddress(peerAddr), 0, 0)
		if err := f.Error(); err != nil {
			http.Error(w, "Join failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Node %s joined successfully", peerID)
	})

	// === 3D Printer API Endpoints ===
	
	// Printer endpoints
	http.HandleFunc("/api/v1/printers", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			if node.raft.State() != raft.Leader {
				redirectToLeader(w, r, node.raft.Leader())
				return
			}
			
			var printer Printer
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}
			
			if err := json.Unmarshal(body, &printer); err != nil {
				http.Error(w, "Invalid printer data", http.StatusBadRequest)
				return
			}
			
			// Generate ID if not provided
			if printer.ID == "" {
				printer.ID = uuid.New().String()
			}
			
			cmd := command{
				Op:   "add_printer",
				Data: printer,
			}
			
			data, err := json.Marshal(cmd)
			if err != nil {
				http.Error(w, "Failed to marshal command", http.StatusInternalServerError)
				return
			}
			
			// Apply to raft
			applyFuture := node.raft.Apply(data, 5*time.Second)
			if err := applyFuture.Error(); err != nil {
				http.Error(w, "Raft apply failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(printer)
			
		case "GET":
			node.store.mu.Lock()
			printers := make([]Printer, 0, len(node.store.store.Printers))
			for _, printer := range node.store.store.Printers {
				printers = append(printers, printer)
			}
			node.store.mu.Unlock()
			
			json.NewEncoder(w).Encode(printers)
			
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	
	// Filament endpoints
	http.HandleFunc("/api/v1/filaments", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			if node.raft.State() != raft.Leader {
				redirectToLeader(w, r, node.raft.Leader())
				return
			}
			
			var filament Filament
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}
			
			if err := json.Unmarshal(body, &filament); err != nil {
				http.Error(w, "Invalid filament data", http.StatusBadRequest)
				return
			}
			
			// Validate filament type
			validTypes := map[string]bool{"PLA": true, "PETG": true, "ABS": true, "TPU": true}
			if !validTypes[filament.Type] {
				http.Error(w, "Invalid filament type. Must be one of: PLA, PETG, ABS, TPU", http.StatusBadRequest)
				return
			}
			
			// Generate ID if not provided
			if filament.ID == "" {
				filament.ID = uuid.New().String()
			}
			
			// Set remaining weight equal to total weight if not specified
			if filament.RemainingWeightInGrams == 0 {
				filament.RemainingWeightInGrams = filament.TotalWeightInGrams
			}
			
			cmd := command{
				Op:   "add_filament",
				Data: filament,
			}
			
			data, err := json.Marshal(cmd)
			if err != nil {
				http.Error(w, "Failed to marshal command", http.StatusInternalServerError)
				return
			}
			
			// Apply to raft
			applyFuture := node.raft.Apply(data, 5*time.Second)
			if err := applyFuture.Error(); err != nil {
				http.Error(w, "Raft apply failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(filament)
			
		case "GET":
			node.store.mu.Lock()
			filaments := make([]Filament, 0, len(node.store.store.Filaments))
			for _, filament := range node.store.store.Filaments {
				filaments = append(filaments, filament)
			}
			node.store.mu.Unlock()
			
			json.NewEncoder(w).Encode(filaments)
			
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	
	// Print Job endpoints
	http.HandleFunc("/api/v1/print_jobs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			if node.raft.State() != raft.Leader {
				redirectToLeader(w, r, node.raft.Leader())
				return
			}
			
			var printJob PrintJob
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}
			
			if err := json.Unmarshal(body, &printJob); err != nil {
				http.Error(w, "Invalid print job data", http.StatusBadRequest)
				return
			}
			
			// Generate ID if not provided
			if printJob.ID == "" {
				printJob.ID = uuid.New().String()
			}
			
			// Set initial status
			printJob.Status = "Queued"
			
			// Validate printer exists
			node.store.mu.Lock()
			_, printerExists := node.store.store.Printers[printJob.PrinterID]
			if !printerExists {
				node.store.mu.Unlock()
				http.Error(w, "Printer not found", http.StatusBadRequest)
				return
			}
			
			// Validate filament exists and has enough weight
			filament, filamentExists := node.store.store.Filaments[printJob.FilamentID]
			if !filamentExists {
				node.store.mu.Unlock()
				http.Error(w, "Filament not found", http.StatusBadRequest)
				return
			}
			
			// Calculate remaining weight after accounting for queued/running jobs
			remainingWeight := filament.RemainingWeightInGrams
			for _, job := range node.store.store.PrintJobs {
				if job.FilamentID == printJob.FilamentID && (job.Status == "Queued" || job.Status == "Running") {
					remainingWeight -= job.PrintWeightInGrams
				}
			}
			node.store.mu.Unlock()
			
			if printJob.PrintWeightInGrams > remainingWeight {
				http.Error(w, "Not enough filament remaining for this print job", http.StatusBadRequest)
				return
			}
			
			cmd := command{
				Op:   "add_print_job",
				Data: printJob,
			}
			
			data, err := json.Marshal(cmd)
			if err != nil {
				http.Error(w, "Failed to marshal command", http.StatusInternalServerError)
				return
			}
			
			// Apply to raft
			applyFuture := node.raft.Apply(data, 5*time.Second)
			if err := applyFuture.Error(); err != nil {
				http.Error(w, "Raft apply failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(printJob)
			
		case "GET":
			// Get optional status filter
			statusFilter := r.URL.Query().Get("status")
			
			node.store.mu.Lock()
			printJobs := make([]PrintJob, 0)
			for _, job := range node.store.store.PrintJobs {
				if statusFilter == "" || job.Status == statusFilter {
					printJobs = append(printJobs, job)
				}
			}
			node.store.mu.Unlock()
			
			json.NewEncoder(w).Encode(printJobs)
			
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	
	// Print Job Status Update endpoint
	http.HandleFunc("/api/v1/print_jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		
		if !strings.Contains(r.URL.Path, "/status") {
			http.Error(w, "Invalid endpoint", http.StatusNotFound)
			return
		}
		
		// Extract job ID from URL path
		pathParts := strings.Split(r.URL.Path, "/")
		if len(pathParts) < 5 {
			http.Error(w, "Invalid URL format", http.StatusBadRequest)
			return
		}
		jobID := pathParts[4]
		
		// Get status from query params
		status := r.URL.Query().Get("status")
		if status == "" {
			http.Error(w, "Status parameter required", http.StatusBadRequest)
			return
		}
		
		// Validate status
		validStatuses := map[string]bool{"Running": true, "Done": true, "Canceled": true}
		if !validStatuses[status] {
			http.Error(w, "Invalid status. Must be one of: Running, Done, Canceled", http.StatusBadRequest)
			return
		}
		
		if node.raft.State() != raft.Leader {
			redirectToLeader(w, r, node.raft.Leader())
			return
		}
		
		// Validate job exists and check current status
		node.store.mu.Lock()
		job, exists := node.store.store.PrintJobs[jobID]
		if !exists {
			node.store.mu.Unlock()
			http.Error(w, "Print job not found", http.StatusNotFound)
			return
		}
		
		currentStatus := job.Status
		node.store.mu.Unlock()
		
		// Validate status transition
		valid := false
		switch status {
		case "Running":
			valid = currentStatus == "Queued"
		case "Done":
			valid = currentStatus == "Running"
		case "Canceled":
			valid = currentStatus == "Queued" || currentStatus == "Running"
		}
		
		if !valid {
			http.Error(w, fmt.Sprintf("Invalid status transition from %s to %s", currentStatus, status), http.StatusBadRequest)
			return
		}
		
		// Create and apply command
		cmd := command{
			Op:       "update_print_job_status",
			ObjectID: jobID,
			Data:     status,
		}
		
		data, err := json.Marshal(cmd)
		if err != nil {
			http.Error(w, "Failed to marshal command", http.StatusInternalServerError)
			return
		}
		
		// Apply to raft
		applyFuture := node.raft.Apply(data, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			http.Error(w, "Raft apply failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		
		// Return updated job
		node.store.mu.Lock()
		updatedJob := node.store.store.PrintJobs[jobID]
		node.store.mu.Unlock()
		
		json.NewEncoder(w).Encode(updatedJob)
	})
	
	log.Printf("[HTTP] Listening on port %s", *httpPort)
	log.Fatal(http.ListenAndServe(":"+*httpPort, nil))
}

// Helper function to redirect requests to the current leader
func redirectToLeader(w http.ResponseWriter, r *http.Request, leaderAddr raft.ServerAddress) {
	leaderParts := strings.Split(string(leaderAddr), ":")
	if len(leaderParts) != 2 {
		http.Error(w, "Current leader address is invalid", http.StatusInternalServerError)
		return
	}
	
	// Extract leader's port information
	raftPort := leaderParts[1]
	
	// Convert raft port to HTTP port (add 4000 to raft port convention)
	httpPortInt, err := strconv.Atoi(raftPort)
	if err != nil {
		http.Error(w, "Invalid leader port", http.StatusInternalServerError)
		return
	}
	httpPort := httpPortInt - 4000
	
	// Build redirect URL
	redirectURL := fmt.Sprintf("http://%s:%d%s", leaderParts[0], httpPort, r.URL.Path)
	if r.URL.RawQuery != "" {
		redirectURL += "?" + r.URL.RawQuery
	}
	
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}