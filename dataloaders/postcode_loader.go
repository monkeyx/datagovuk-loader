package dataloaders

import (
	"github.com/jinzhu/gorm"
)

// PostCodeLoader is a data loader for ONS Post Code N-Triples format.
type PostCodeLoader struct {}

// Loads post code data
func (p PostCodeLoader) Load(db *gorm.DB) (err error) {
	db.AutoMigrate(&PostCodeUnit{})
	db.AutoMigrate(&PostCodeArea{})
	db.AutoMigrate(&PostCodeSector{})
	db.AutoMigrate(&PostCodeDistrict{})

	ch := make(chan bool)

	go FetchAll(ch, db, &PostCodeDistrictFetcher{})

	go FetchAll(ch, db, &PostCodeSectorFetcher{})

	go FetchAll(ch, db, &PostCodeAreaFetcher{})

	go FetchAll(ch, db, &PostCodeUnitFetcher{})

	count := 0
	for {
		
		if f := <- ch; f {
			count += 1
		}

		if count > 3 {
			break
		}
	}

	return nil
}