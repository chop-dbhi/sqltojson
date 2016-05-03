package sqltojson

import (
	"github.com/chop-dbhi/sql-agent"
	"github.com/jmoiron/sqlx"
)

type Params map[string]interface{}

func BuildParams(rec sqlagent.Record, keys []string) Params {
	params := make(Params, len(keys))

	for _, key := range keys {
		params[key] = rec[key]
	}

	return params
}

func FetchAll(db *sqlx.DB, query string, params Params) ([]sqlagent.Record, error) {
	iter, err := sqlagent.Execute(db, query, params)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var recs []sqlagent.Record

	for iter.Next() {
		rec := make(sqlagent.Record)

		if err := iter.Scan(rec); err != nil {
			return nil, err
		}

		recs = append(recs, rec)
	}

	return recs, nil
}

func Build(db *sqlx.DB, schema *Schema, rec sqlagent.Record) error {
	if schema.Nested != nil {
		// Params for sub-query.
		params := BuildParams(rec, schema.Key)

		for key, nested := range schema.Nested {
			sql := nested.SQL

			children, err := FetchAll(db, sql, params)
			if err != nil {
				return err
			}

			rec[key] = children

			// Recurse on each child.
			for _, child := range children {
				if err := Build(db, nested, child); err != nil {
					return err
				}
			}

			schema.SetProperty(key, nested.Mapping)
		}
	}

	for _, field := range schema.Exclude {
		delete(rec, field)
	}

	schema.InferMapping(rec)

	return nil
}

type BuildTask struct {
	Schema *Schema
	Record sqlagent.Record
}
