package dataloaders

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"log"
)

// Reads a URL into a byte slice
func ReadUrl(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	log.Println(url + ":", resp.Status)

	body, err := ioutil.ReadAll(resp.Body)

	// log.Println(url + ":", string(body))

	return body, err
}

// Parses JSON into interface
func ParseJSON(body []byte, v interface{}) error {
	return json.Unmarshal(body, v)
}