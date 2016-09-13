package dataloaders

import (
	"bytes"
	"encoding/json"
	"encoding/csv"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Reads a URL into a byte slice
func ReadUrl(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New(url + ": " + resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)

	// log.Println(url + ":", string(body))

	return body, err
}

// Parses JSON into interface
func ParseJSON(body []byte, v interface{}) error {
	return json.Unmarshal(body, v)
}

// Parses CSV into a slice of maps using the header row to determine the keys
func ParseCSV(body []byte) ([]map[string]string, error) {
	slice := make([]map[string]string, 0)
	records, err := csv.NewReader(bytes.NewReader(body)).ReadAll()

	if err != nil {
		return slice, err
	}

	rows := len(records)
	for i := 1; i < rows; i++ {
		m := make(map[string]string)
		cols := len(records[i])
		for j := 0; j < cols; j++ {
			m[records[0][j]] = records[i][j]
		}
		slice = append(slice, m)
	}

	return slice, nil
}

const SimpleDateFormat = "02/01/2006"

// Parses a simple DAY/MONTH/YEAR date format into a Time
func ParseSimpleDate(date string) (t time.Time, err error) {
	return time.Parse(SimpleDateFormat, date)
}

// Prints a map to log file
func PrintMap(m map[string]string) {
	for k := range m {
	    log.Println("\t",k,"=",m[k])
	}
}