package sqltojson

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/chop-dbhi/sql-agent"
	"golang.org/x/net/context"
)

func Signaler(cxt context.Context, cancel context.CancelFunc) {
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, os.Kill)

	for {
		select {
		case <-signals:
			cancel()
			log.Print("Signal caught. Canceling.")
			return
		}
	}
}

func StatsWriter(cxt context.Context, output io.Writer, ch <-chan time.Duration) {
	var (
		first bool
		start time.Time
		dur   time.Duration
		count int64
	)

	for {
		select {
		case <-cxt.Done():
			log.Print("Stopping stats writer.")
			return

		case v, ok := <-ch:
			if !ok {
				return
			}

			count += 1
			dur += v

			if !first {
				first = true
				start = time.Now()
				continue
			}

			build := float64(count) / float64(dur/time.Second)
			conc := float64(count) / float64(time.Now().Sub(start)/time.Second)
			rate := conc * build

			fmt.Fprintf(output, "\rCNT: %d, ABT: %f, CONC: %f, BPS: %f", count, build, conc, rate)
		}
	}
}

func DataWriter(cxt context.Context, config *Config, output io.Writer, ch <-chan sqlagent.Record) {
	action := map[string]interface{}{
		"create": map[string]string{
			"_index": config.Index,
			"_type":  config.Type,
		},
	}

	var err error

	encoder := json.NewEncoder(output)

	for {
		select {
		case <-cxt.Done():
			log.Print("Stopping data writer.")
			return

		case rec, ok := <-ch:
			if !ok {
				return
			}

			if err = encoder.Encode(action); err != nil {
				log.Println("Error encoding action")
				continue
			}

			if err = encoder.Encode(rec); err != nil {
				log.Println("Error encoding record")
				continue
			}
		}
	}
}

type Worker struct {
	ID     int
	config *Config
	queue  <-chan *BuildTask
	output chan<- sqlagent.Record
	stats  chan<- time.Duration
}

func (w *Worker) Start(cxt context.Context) {
	for {
		select {
		case <-cxt.Done():
			log.Printf("Stopping worker #%d.", w.ID)
			return

		case task, ok := <-w.queue:
			if !ok {
				return
			}

			// Jitter.
			jitter := float32(time.Millisecond) * 100 * rand.Float32()
			time.Sleep(time.Duration(jitter))

			// Capture build time.
			var t0 time.Time

			var i int

			for {
				if i == w.config.MaxRetries {
					log.Fatalf("Reached max retries. Exiting")
					break
				}

				t0 = time.Now()

				if err := Build(w.config.DB, task.Schema, task.Record); err != nil {
					log.Printf("Error %s.\nRetrying...", err)
					time.Sleep(time.Second * 2)
					i++
					continue
				}

				break
			}

			select {
			case <-cxt.Done():
				break

			case w.output <- task.Record:
				w.stats <- time.Now().Sub(t0)
			}
		}
	}
}

func ReadSource(cxt context.Context, config *Config, queue chan<- *BuildTask) {
	iter, err := sqlagent.Execute(config.DB, config.Schema.SQL, nil)

	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}

	defer iter.Close()

	// Execute the top-level query and fill the queue.
	log.Printf("Fetching root '%s' objects", config.Schema.Type)

	var count int
	for iter.Next() {
		rec := make(sqlagent.Record)

		if err := iter.Scan(rec); err != nil {
			log.Printf("Error scanning record: %s", err)
			continue
		}

		// Attempt to write to the queue or timeout.
		select {
		case <-cxt.Done():
			log.Print("Stopping source reader.")
			return

		case queue <- &BuildTask{config.Schema, rec}:
			count += 1
		}
	}

	log.Printf("Queued %d objects", count)
}

func WriteMapping(schema *Schema, output io.Writer) error {
	val := map[string]interface{}{
		"mappings": map[string]interface{}{
			schema.Type: map[string]interface{}{
				"properties": schema.Mapping.Properties,
			},
		},
	}

	b, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		return fmt.Errorf("Error marshaling mapping JSON: %s", err)
	}

	if _, err := output.Write(b); err != nil {
		return fmt.Errorf("Error writing to file: %s", err)
	}

	return nil
}
