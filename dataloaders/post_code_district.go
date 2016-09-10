package dataloaders

import (
	"errors"
	"github.com/jinzhu/gorm"
	"strconv"
	"time"
)

// JSON response structure for PostCode District
type PostCodeDistrictResponse struct {
	Id string `json:"@id"`
	Labels []XmlValue `json:"http://www.w3.org/2000/01/rdf-schema#label"`
	Within []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/within"`
	Contains []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/contains"`
}

// Stringer for PostCodeDistrictResponse
func (p PostCodeDistrictResponse) String() string {
	return p.Id
}

// PostCode unit database model
type PostCodeDistrict struct {
	ID string `gorm:"primary_key"`
	Label string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

// Stringer for PostCodeUnitResponse
func (p PostCodeDistrict) String() string {
	return p.ID
}

// PostCodeArea fetcher
type PostCodeDistrictFetcher struct {
	Results []PostCodeDistrictResponse
	TotalResults int
}

// Base URL
func (p *PostCodeDistrictFetcher) BaseUrl() string {
	return PostCodeDistrictUrl;
}

// Parse JSON results
func (p *PostCodeDistrictFetcher) ParseResults(body []byte) (int, error) {
	err := ParseJSON(body,&p.Results)
	return len(p.Results), err
}

func (p *PostCodeDistrictFetcher) SaveOrUpdate(db *gorm.DB, index int) error {
	if index >= len(p.Results) {
		return errors.New("Invalid index: " + strconv.Itoa(index))
	} 
	r := p.Results[index]
	poa := PostCodeDistrict{}
	db.Where("ID = ?", r.Id).First(&poa)
	area := &PostCodeDistrict{ID: r.Id, Label: FirstOrEmptyXmlValue(r.Labels)}

	if poa.ID == "" {
		err := db.Create(area).Error

		if err != nil {
			return err
		}
	} else {
		err := db.Save(area).Error

		if err != nil {
			return err
		}
	}
	return nil
}
