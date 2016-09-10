package dataloaders

import (
	"errors"
	"github.com/jinzhu/gorm"
	"strconv"
	"time"
)

// JSON response structure for PostCode Sector
type PostCodeSectorResponse struct {
	Id string `json:"@id"`
	Labels []XmlValue `json:"http://www.w3.org/2000/01/rdf-schema#label"`
	Contains []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/contains"`
}

// Stringer for PostCodeSectorResponse
func (p PostCodeSectorResponse) String() string {
	return p.Id
}

// PostCode unit database model
type PostCodeSector struct {
	ID string `gorm:"primary_key"`
	Label string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

// Stringer for PostCodeUnitResponse
func (p PostCodeSector) String() string {
	return p.ID
}

// PostCodeArea fetcher
type PostCodeSectorFetcher struct {
	Results []PostCodeSectorResponse
	TotalResults int
}

// Base URL
func (p *PostCodeSectorFetcher) BaseUrl() string {
	return PostCodeSectorUrl;
}

// Parse JSON results
func (p *PostCodeSectorFetcher) ParseResults(body []byte) (int, error) {
	err := ParseJSON(body,&p.Results)
	return len(p.Results), err
}

func (p *PostCodeSectorFetcher) SaveOrUpdate(db *gorm.DB, index int) error {
	if index >= len(p.Results) {
		return errors.New("Invalid index: " + strconv.Itoa(index))
	} 
	r := p.Results[index]
	poa := PostCodeSector{}
	db.Where("ID = ?", r.Id).First(&poa)
	area := &PostCodeSector{ID: r.Id, Label: FirstOrEmptyXmlValue(r.Labels)}

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
