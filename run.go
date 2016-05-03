package sqltojson

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/chop-dbhi/sql-agent"

	"golang.org/x/net/context"
)

func Run(config *Config) error {
	defer config.DB.Close()

	startTime := time.Now()

	// Set up primary context and signal handling.
	// All routines use the context to check if the work is being canceled.
	cxt, cancel := context.WithCancel(context.Background())

	// Start a routine for catching OS signals. If caught it calls cancel.
	go Signaler(cxt, cancel)

	// Initialize the channels to communicate between goroutines.
	// Workers send stats and a stats handler computes metrics to print to stderr.
	stats := make(chan time.Duration, config.Workers)

	// The queue is filled by the source reader and workers read from the queue
	// and build the records.
	queue := make(chan *BuildTask, config.Workers)

	// The output channel is where workers send a complete record. The data writer
	// reads from the channel and writes it to the data file.
	output := make(chan sqlagent.Record, config.Workers)

	// Wait group for non-worker routines.
	wg := &sync.WaitGroup{}

	// Start routine for write stats to stderr.
	// This writer is done after the queue is exhausted.
	wg.Add(1)
	go func() {
		defer wg.Done()
		StatsWriter(cxt, os.Stderr, stats)
	}()

	var dataFile io.Writer
	if config.Files.Data == "-" {
		dataFile = os.Stdout
	} else {
		file, err := os.Create(config.Files.Data)
		if err != nil {
			return fmt.Errorf("Error creating data file: %s", err)
		}
		defer file.Close()
		dataFile = file
	}

	// Routine that prints the documents.
	// This routine is done once the output channel is exhausted.
	wg.Add(1)
	go func() {
		defer wg.Done()
		DataWriter(cxt, config, dataFile, output)
	}()

	// Start the workers.
	log.Printf("Starting %d workers", config.Workers)

	// Separate worker wait group so the output can be closed once all
	// workers are done.
	wwg := &sync.WaitGroup{}

	wwg.Add(config.Workers)
	for i := 0; i < config.Workers; i++ {
		go func(i int) {
			defer wwg.Done()
			w := &Worker{
				ID:     i + 1,
				config: config,
				queue:  queue,
				output: output,
				stats:  stats,
			}
			w.Start(cxt)
		}(i)
	}

	// Read from the source and populate the queue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ReadSource(cxt, config, queue)
		close(queue)
	}()

	wwg.Wait()

	log.Print("Workers done.")
	close(output)
	close(stats)

	// Wait until the remaining routines are finished.
	wg.Wait()
	log.Println("Data file done.")

	// Write the mapping file.
	mapFile, err := os.Create(config.Files.Mapping)
	if err != nil {
		return fmt.Errorf("Error opening mapping file: %s", err)
	}
	defer mapFile.Close()

	// Write the mapping.
	if err := WriteMapping(config.Schema, mapFile); err != nil {
		return err
	}

	log.Print("Wrote mapping file.")
	log.Printf("Took %s.\n", time.Now().Sub(startTime))

	return nil
}
