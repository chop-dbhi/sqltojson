package sqltojson

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/chop-dbhi/sql-agent"
	"github.com/jmoiron/sqlx"
)

var (
	defaultDataFile    = "data.json"
	defaultMappingFile = "mapping.json"
	defaultWorkers     = 10
)

type Mapping struct {
	Type       string              `json:"type"`
	Index      string              `json:"index,omitempty"`
	Format     string              `json:"format,omitempty"`
	Properties map[string]*Mapping `json:"properties,omitempty"`
}

type Schema struct {
	Type    string
	Key     []string
	Exclude []string
	SQL     string
	Params  map[string]interface{}
	Nested  map[string]*Schema
	Mapping *Mapping

	mux sync.Mutex
}

func (s *Schema) Validate() error {
	s.Mapping = &Mapping{
		Type:       "nested",
		Properties: make(map[string]*Mapping),
	}

	if s.Nested != nil || len(s.Nested) > 0 {
		if len(s.Key) == 0 {
			return fmt.Errorf("A key is required to included nested objects.")
		}

		for n, c := range s.Nested {
			if err := c.Validate(); err != nil {
				return fmt.Errorf("%s/%s: %s", s.Type, n, err)
			}
		}
	}

	return nil
}

func (s *Schema) SetProperty(key string, prop *Mapping) {
	s.mux.Lock()
	s.Mapping.Properties[key] = prop
	s.mux.Unlock()
}

func (s *Schema) hasProp(key string) bool {
	s.mux.Lock()
	_, ok := s.Mapping.Properties[key]
	s.mux.Unlock()
	return ok
}

func (s *Schema) setProp(key string, val interface{}) {
	prop := &Mapping{}

	switch x := val.(type) {
	case []sqlagent.Record:
		prop.Type = "nested"

	case string:
		prop.Type = "string"

	case bool:
		prop.Type = "boolean"

	case byte, int8:
		prop.Type = "byte"

	case int16:
		prop.Type = "short"

	case int, int32:
		prop.Type = "integer"

	case int64:
		prop.Type = "long"

	case float32:
		prop.Type = "float"

	case float64:
		prop.Type = "double"

	case time.Time:
		prop.Type = "date"

	default:
		fmt.Fprintf(os.Stderr, "unknown type %T for %s/%s\n", x, s.Type, key)
	}

	s.SetProperty(key, prop)
}

func (s *Schema) InferMapping(rec sqlagent.Record) {
	for key, val := range rec {
		if s.hasProp(key) {
			continue
		}

		if val == nil {
			continue
		}

		s.setProp(key, val)
	}
}

type Config struct {
	URL        string
	Connection struct {
		Driver string
		Params map[string]interface{}
	}
	Files struct {
		Data    string
		Mapping string
	}
	Workers     int
	Connections int
	Index       string
	Type        string
	Schema      *Schema
	DB          *sqlx.DB
}

func (c *Config) Validate() error {
	return c.Schema.Validate()
}

func newConfig() *Config {
	c := &Config{
		Workers:     defaultWorkers,
		Connections: defaultWorkers,
	}

	c.Files.Data = defaultDataFile
	c.Files.Mapping = defaultMappingFile

	return c
}

func ReadConfig(path string) (*Config, error) {
	// Open, read and validate the config.
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Could not open config file: %s", err)
	}
	defer f.Close()

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("Error reading config file: %s", err)
	}

	c := newConfig()
	if err = yaml.Unmarshal(bytes, c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %s", err)
	}

	if err = c.Validate(); err != nil {
		return nil, fmt.Errorf("Config failed validated: %s", err)
	}

	// Connect to the database.
	db, err := sqlagent.Connect(c.Connection.Driver, c.Connection.Params)
	if err != nil {
		return nil, fmt.Errorf("Error with database connection: %s", err)
	}

	db.SetMaxOpenConns(c.Connections)

	c.DB = db

	return c, nil
}
