package dataloaders

import (
	"errors"
	"github.com/jinzhu/gorm"
	"strconv"
	"strings"
	"time"
)

const PostCodeSectorUrl = "http://opendatacommunities.org/resources.json?dataset=postcodes&type_uri=http%3A%2F%2Fdata.ordnancesurvey.co.uk%2Fontology%2Fpostcode%2FPostcodeSector"

// JSON response structure for PostCode Sector
type PostCodeSectorResponse struct {
	Id string `json:"@id"`
	Labels []XmlValue `json:"http://www.w3.org/2000/01/rdf-schema#label"`
	Contains []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/contains"`
	Within []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/within"`
}

// Stringer for PostCodeSectorResponse
func (p PostCodeSectorResponse) String() string {
	return p.Id
}

// PostCode unit database model
type PostCodeSector struct {
	ID string `gorm:"primary_key"`
	DistrictID string `gorm:"index"`
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

// Stringer for PostCodeSectorFetcher
func (p PostCodeSectorFetcher) String() string {
	return "PostCode Sector Fetcher"
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

// Create or save results at specified index
func (p *PostCodeSectorFetcher) CreateOrSave(db *gorm.DB, index int) error {
	if index >= len(p.Results) {
		return errors.New("Invalid index: " + strconv.Itoa(index))
	} 
	r := p.Results[index]
	poa := PostCodeSector{}
	db.Where("ID = ?", r.Id).First(&poa)
	sector := &PostCodeSector{ID: r.Id, Label: FirstOrEmptyXmlValue(r.Labels)}

	c := len(r.Within)
	for i := 0; i < c; i++ {
		if strings.Count(r.Within[i].Id, "postcodedistrict") > 0 {
			sector.DistrictID = r.Within[i].Id
		}
	}

	if poa.ID == "" {
		err := db.Create(sector).Error

		if err != nil {
			return err
		}
	} else {
		err := db.Save(sector).Error

		if err != nil {
			return err
		}
	}
	return nil
}
