package dataloaders

import (
	"github.com/jinzhu/gorm"
)

const PerPage = 250
const PostCodeUnitUrl = "http://opendatacommunities.org/resources.json?dataset=postcodes&type_uri=http%3A%2F%2Fdata.ordnancesurvey.co.uk%2Fontology%2Fpostcode%2FPostcodeUnit"
const PostCodeAreaUrl = "http://opendatacommunities.org/resources.json?dataset=postcodes&type_uri=http%3A%2F%2Fdata.ordnancesurvey.co.uk%2Fontology%2Fpostcode%2FPostcodeArea"
const PostCodeSectorUrl = "http://opendatacommunities.org/resources.json?dataset=postcodes&type_uri=http%3A%2F%2Fdata.ordnancesurvey.co.uk%2Fontology%2Fpostcode%2FPostcodeSector"
const PostCodeDistrictUrl = "http://opendatacommunities.org/resources.json?dataset=postcodes&type_uri=http%3A%2F%2Fdata.ordnancesurvey.co.uk%2Fontology%2Fpostcode%2FPostcodeDistrict"

// PostCodeLoader is a data loader for ONS Post Code N-Triples format.
type PostCodeLoader struct {}

// Loads post code data
func (p PostCodeLoader) Load(db *gorm.DB) (err error) {
	db.AutoMigrate(&PostCodeUnit{})
	db.AutoMigrate(&PostCodeArea{})
	db.AutoMigrate(&PostCodeSector{})
	db.AutoMigrate(&PostCodeDistrict{})

	// TODO - make concurrent

	_, err = FetchAll(db, &PostCodeDistrictFetcher{})

	if err != nil {
		return err
	}

	_, err = FetchAll(db, &PostCodeSectorFetcher{})

	if err != nil {
		return err
	}

	_, err = FetchAll(db, &PostCodeAreaFetcher{})

	if err != nil {
		return err
	}

	_, err = FetchAll(db, &PostCodeUnitFetcher{})

	if err != nil {
		return err
	}

	return nil
}