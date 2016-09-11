package dataloaders

import (
	"log"
	"strconv"
	"github.com/jinzhu/gorm"
)

const PerPage = 250

// Fetcher is an interface for fetching JSON data
type Fetcher interface {
	BaseUrl() string
	ParseResults(body []byte) (int, error)
	SaveOrUpdate(db *gorm.DB, index int) error
}

// Fetches all pages using a fetcher
func FetchAll(db *gorm.DB, f Fetcher) (int, error) {
	total := 0
	page := 1
	for {
		c, err := Fetch(db, f, page)
		if c < 1 {
			if total < 1 {
				return total, err
			}
			break
		}
		total += c
		page += 1
	}
	log.Println("Fetched: ", total)
	return total, nil
}

// Fetches one page with the help of a Fetcher
func Fetch(db *gorm.DB, f Fetcher, page int) (int, error) {
	url := f.BaseUrl()  + "&page=" + strconv.Itoa(page) + "&per_page=" + strconv.Itoa(PerPage)
	body, err := ReadUrl(url)

	if err != nil {
		return 0, err
	} 

	c, err := f.ParseResults(body)

	if err != nil {
		return 0, err
	}

	tx := db.Begin()

	// log.Println("COUNT: ", c)
	for i := 0; i < c; i++ {
		err = f.SaveOrUpdate(db, i)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	tx.Commit()
	return c, nil
}