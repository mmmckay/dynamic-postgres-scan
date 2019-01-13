package main

import (
	"database/sql"
	"reflect"
	"strconv"

	"github.com/lib/pq"
)

// DynamicResultMaps generates a slice of maps from a set of rows, with columns names as keys and row values as values
// Can currently handle the following PostgreSQL fields:
// integer, text, numeric, decimal, uuid, serial, boolean, integer[], text[], numeric[], decimal[], boolean[], uuid[]
func DynamicResultMaps(rows *sql.Rows) (results []map[string]interface{}, err error) {
	// get column types, names
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return
	}

	// iterate rows
	for rows.Next() {
		// generate value and pointer slices for the current row
		values := make([]interface{}, len(columnTypes))
		valuePtrs := make([]interface{}, len(columnTypes))
		for i, c := range columnTypes {
			values[i] = reflect.Zero(c.ScanType())
			valuePtrs[i] = &values[i]
		}

		// scan to pointers
		rows.Scan(valuePtrs...)

		// array conversion and map building
		rowResult := make(map[string]interface{})
		for i, c := range columnTypes {
			switch c.DatabaseTypeName() {
			// text and uuid array
			case "_TEXT", "_UUID":
				array := []string{}
				arrayPtr := pq.Array(&array)
				arrayPtr.Scan(values[i].([]byte))
				rowResult[c.Name()] = array
			// float array
			case "_NUMERIC", "_DECIMAL":
				array := []float64{}
				arrayPtr := pq.Array(&array)
				arrayPtr.Scan(values[i].([]byte))
				rowResult[c.Name()] = array
			// integer array
			case "_INT4":
				array := []int64{}
				arrayPtr := pq.Array(&array)
				arrayPtr.Scan(values[i].([]byte))
				rowResult[c.Name()] = array
			// bool array
			case "_BOOL":
				array := []bool{}
				arrayPtr := pq.Array(&array)
				arrayPtr.Scan(values[i].([]byte))
				rowResult[c.Name()] = array
			// float
			case "NUMERIC", "DECIMAL":
				f, err := strconv.ParseFloat(string(values[i].([]byte)), 64)
				if err != nil {
					// what?
					continue
				}
				rowResult[c.Name()] = f
			// uuid
			case "UUID":
				rowResult[c.Name()] = string(values[i].([]byte))
			// default types (int, string, bool, serial)
			default:
				rowResult[c.Name()] = values[i]
			}
		}

		results = append(results, rowResult)
	}

	return
}

// DynamicResultSlices returns a slice of slices containing the dyanamically type cast results from sql rows
// Can currently handle the following PostgreSQL fields:
// integer, text, numeric, decimal, uuid, serial, boolean, integer[], text[], numeric[], decimal[], boolean[], uuid[]
func DynamicResultSlices(rows *sql.Rows) (results [][]interface{}, err error) {
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	mapResults, err := DynamicResultMaps(rows)
	if err != nil {
		return
	}

	for _, m := range mapResults {
		newSlice := make([]interface{}, len(columns))
		for i, name := range columns {
			newSlice[i] = m[name]
		}
		results = append(results, newSlice)
	}

	return
}
