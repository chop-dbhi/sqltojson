package main

import (
	"flag"
	"log"

	"github.com/chop-dbhi/sqltojson"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
)

var buildVersion string

func main() {
	var (
		configName     string
		numWorkers     int
		numConnections int
	)

	flag.StringVar(&configName, "config", "sqltojson.yaml", "Path to configuration file.")
	flag.IntVar(&numWorkers, "workers", 0, "Numbers of workers override.")
	flag.IntVar(&numConnections, "connections", 0, "Max number of connections override.")

	flag.Parse()

	// Read and validate config.
	config, err := sqltojson.ReadConfig(configName)
	if err != nil {
		log.Fatal(err)
	}

	// Override.
	if numWorkers > 0 {
		config.Workers = numWorkers
	}

	if numConnections > 0 {
		config.Connections = numConnections
	}

	if err := sqltojson.Run(config); err != nil {
		log.Fatal(err)
	}
}
