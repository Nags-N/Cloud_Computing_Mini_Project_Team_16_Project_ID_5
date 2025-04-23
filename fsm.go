package main

import (
	"encoding/json"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

// Data Models
type Printer struct {
	ID      string `json:"id"`
	Company string `json:"company"`
	Model   string `json:"model"`
}

type Filament struct {
	ID                     string `json:"id"`
	Type                   string `json:"type"`
	Color                  string `json:"color"`
	TotalWeightInGrams     int    `json:"total_weight_in_grams"`
	RemainingWeightInGrams int    `json:"remaining_weight_in_grams"`
}

type PrintJob struct {
	ID                string `json:"id"`
	PrinterID         string `json:"printer_id"`
	FilamentID        string `json:"filament_id"`
	Filepath          string `json:"filepath"`
	PrintWeightInGrams int   `json:"print_weight_in_grams"`
	Status            string `json:"status"`  // Queued, Running, Done, Canceled
}

// Store structure
type Store struct {
	Printers  map[string]Printer  `json:"printers"`
	Filaments map[string]Filament `json:"filaments"`
	PrintJobs map[string]PrintJob `json:"print_jobs"`
}

// FSM implementation
type fsm struct {
	mu    sync.Mutex
	store Store
}

// Command structure for Raft operations
type command struct {
	Op       string      `json:"op"`
	ObjectID string      `json:"object_id,omitempty"`
	Data     interface{} `json:"data,omitempty"`
}

// Apply Raft log entry to state machine
func (f *fsm) Apply(l *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("Failed to unmarshal command: %v", err)
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Initialize maps if they don't exist
	if f.store.Printers == nil {
		f.store.Printers = make(map[string]Printer)
	}
	if f.store.Filaments == nil {
		f.store.Filaments = make(map[string]Filament)
	}
	if f.store.PrintJobs == nil {
		f.store.PrintJobs = make(map[string]PrintJob)
	}

	switch cmd.Op {
	case "add_printer":
		var printer Printer
		data, err := json.Marshal(cmd.Data)
		if err != nil {
			log.Printf("Failed to marshal printer data: %v", err)
			return nil
		}
		if err := json.Unmarshal(data, &printer); err != nil {
			log.Printf("Failed to unmarshal printer data: %v", err)
			return nil
		}
		
		f.store.Printers[printer.ID] = printer
		log.Printf("[FSM] Added printer: %s (%s %s)", printer.ID, printer.Company, printer.Model)
		return printer

	case "add_filament":
		var filament Filament
		data, err := json.Marshal(cmd.Data)
		if err != nil {
			log.Printf("Failed to marshal filament data: %v", err)
			return nil
		}
		if err := json.Unmarshal(data, &filament); err != nil {
			log.Printf("Failed to unmarshal filament data: %v", err)
			return nil
		}
		
		f.store.Filaments[filament.ID] = filament
		log.Printf("[FSM] Added filament: %s (%s, %s)", filament.ID, filament.Type, filament.Color)
		return filament

	case "add_print_job":
		var printJob PrintJob
		data, err := json.Marshal(cmd.Data)
		if err != nil {
			log.Printf("Failed to marshal print job data: %v", err)
			return nil
		}
		if err := json.Unmarshal(data, &printJob); err != nil {
			log.Printf("Failed to unmarshal print job data: %v", err)
			return nil
		}
		
		f.store.PrintJobs[printJob.ID] = printJob
		log.Printf("[FSM] Added print job: %s (printer: %s, filament: %s, weight: %d)", 
			printJob.ID, printJob.PrinterID, printJob.FilamentID, printJob.PrintWeightInGrams)
		return printJob

	case "update_print_job_status":
		printJob, exists := f.store.PrintJobs[cmd.ObjectID]
		if !exists {
			log.Printf("[FSM] Print job %s not found", cmd.ObjectID)
			return nil
		}
		
		newStatus, ok := cmd.Data.(string)
		if !ok {
			log.Printf("[FSM] Invalid status data format")
			return nil
		}
		
		oldStatus := printJob.Status
		printJob.Status = newStatus
		f.store.PrintJobs[cmd.ObjectID] = printJob
		
		// If status changed to "Done", update filament weight
		if newStatus == "Done" && oldStatus != "Done" {
			filament, exists := f.store.Filaments[printJob.FilamentID]
			if exists {
				if filament.RemainingWeightInGrams >= printJob.PrintWeightInGrams {
					filament.RemainingWeightInGrams -= printJob.PrintWeightInGrams
					f.store.Filaments[printJob.FilamentID] = filament
					log.Printf("[FSM] Updated filament %s remaining weight to %d", 
						printJob.FilamentID, filament.RemainingWeightInGrams)
				} else {
					log.Printf("[FSM] Warning: Filament %s has insufficient remaining weight", printJob.FilamentID)
					// Still set to Done but log the anomaly
				}
			} else {
				log.Printf("[FSM] Warning: Filament %s not found for print job %s", 
					printJob.FilamentID, cmd.ObjectID)
			}
		}
		
		log.Printf("[FSM] Updated print job %s status from %s to %s", 
			cmd.ObjectID, oldStatus, newStatus)
		return printJob
	}

	return nil
}

// Create snapshot of current state
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Create a deep copy of the store for snapshotting
	storeCopy := Store{
		Printers:  make(map[string]Printer),
		Filaments: make(map[string]Filament),
		PrintJobs: make(map[string]PrintJob),
	}
	
	for k, v := range f.store.Printers {
		storeCopy.Printers[k] = v
	}
	
	for k, v := range f.store.Filaments {
		storeCopy.Filaments[k] = v
	}
	
	for k, v := range f.store.PrintJobs {
		storeCopy.PrintJobs[k] = v
	}
	
	log.Printf("[FSM] Created snapshot with %d printers, %d filaments, %d print jobs", 
		len(storeCopy.Printers), len(storeCopy.Filaments), len(storeCopy.PrintJobs))
	
	return &fsmSnapshot{store: storeCopy}, nil
}

// Restore state from snapshot
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	
	decoder := json.NewDecoder(rc)
	var newStore Store
	if err := decoder.Decode(&newStore); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	
	// Initialize maps if they're nil in the restored data
	if newStore.Printers == nil {
		newStore.Printers = make(map[string]Printer)
	}
	if newStore.Filaments == nil {
		newStore.Filaments = make(map[string]Filament)
	}
	if newStore.PrintJobs == nil {
		newStore.PrintJobs = make(map[string]PrintJob)
	}
	
	f.store = newStore
	log.Printf("[FSM] Restored from snapshot: %d printers, %d filaments, %d print jobs", 
		len(f.store.Printers), len(f.store.Filaments), len(f.store.PrintJobs))
	
	return nil
}

// Snapshot implementation for FSM
type fsmSnapshot struct {
	store Store
}

// Persist snapshot to storage
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.store)
	if err != nil {
		sink.Cancel()
		return err
	}
	
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	
	return sink.Close()
}

// Release resources
func (s *fsmSnapshot) Release() {}