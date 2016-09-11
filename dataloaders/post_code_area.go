package dataloaders

import (
	"errors"
	"github.com/jinzhu/gorm"
	"strconv"
	"time"
)

const PostCodeAreaUrl = "http://opendatacommunities.org/resources.json?dataset=postcodes&type_uri=http%3A%2F%2Fdata.ordnancesurvey.co.uk%2Fontology%2Fpostcode%2FPostcodeArea"

// JSON response structure for PostCode Area
type PostCodeAreaResponse struct {
	Id string `json:"@id"`
	Labels []XmlValue `json:"http://www.w3.org/2000/01/rdf-schema#label"`
	Contains []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/contains"`
}

// Stringer for PostCodeAreaResponse
func (p PostCodeAreaResponse) String() string {
	return p.Id
}

// PostCode unit database model
type PostCodeArea struct {
	ID string `gorm:"primary_key"`
	Label string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

// Stringer for PostCodeUnitResponse
func (p PostCodeArea) String() string {
	return p.ID
}

// PostCodeArea fetcher
type PostCodeAreaFetcher struct {
	Results []PostCodeAreaResponse
	TotalResults int
}

// Stringer for PostCodeAreaFetcher
func (p PostCodeAreaFetcher) String() string {
	return "PostCode Area Fetcher"
}

// Base URL
func (p *PostCodeAreaFetcher) BaseUrl() string {
	return PostCodeAreaUrl;
}

// Parse JSON results
func (p *PostCodeAreaFetcher) ParseResults(body []byte) (int, error) {
	err := ParseJSON(body,&p.Results)
	return len(p.Results), err
}

// Create or save results at specified index
func (p *PostCodeAreaFetcher) CreateOrSave(db *gorm.DB, index int) error {
	if index >= len(p.Results) {
		return errors.New("Invalid index: " + strconv.Itoa(index))
	} 
	r := p.Results[index]
	poa := PostCodeArea{}
	db.Where("ID = ?", r.Id).First(&poa)
	area := &PostCodeArea{ID: r.Id, Label: FirstOrEmptyXmlValue(r.Labels)}

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
