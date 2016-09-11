package dataloaders

import (
	"errors"
	"github.com/jinzhu/gorm"
	"strconv"
	"time"
)

const PostCodeUnitUrl = "http://opendatacommunities.org/resources.json?dataset=postcodes&type_uri=http%3A%2F%2Fdata.ordnancesurvey.co.uk%2Fontology%2Fpostcode%2FPostcodeUnit"

// JSON response structure for PostCode Units
type PostCodeUnitResponse struct {
	Id string `json:"@id"`
	Labels []XmlValue `json:"http://www.w3.org/2000/01/rdf-schema#label"`
	Within []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/within"`
	Latitude []XmlDataType `json:"http://www.w3.org/2003/01/geo/wgs84_pos#lat"`
	Longitude []XmlDataType `json:"http://www.w3.org/2003/01/geo/wgs84_pos#long"`
	Northing []XmlDataType `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/northing"`
	Easting []XmlDataType `json:"http://data.ordnancesurvey.co.uk/ontology/spatialrelations/easting"`
	LE []XmlValue `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/LH"`
	Ward []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/ward"`
	District []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/district"`
	PQ []XmlValue `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/PQ"`
	LSOA []XmlId `json:"http://opendatacommunities.org/def/geography#lsoa"`
	RE []XmlValue `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/RH"`
	Country []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/country"`
	County []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/county"`
	PositionalQualityIndicator []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/positionalQualityIndicator"`
	NHSHealthAuthority []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/nhsHA"`
	NHSRegionalHealthAuthority []XmlId `json:"http://data.ordnancesurvey.co.uk/ontology/postcode/nhsRegionalHA"`
}

// Stringer for PostCodeUnitResponse
func (p PostCodeUnitResponse) String() string {
	return p.Id
}

// PostCode unit database model
type PostCodeUnit struct {
	ID string `gorm:"primary_key"`
	Label string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
	Latitude string
	Longitude string 
	Northing string 
	Easting string 
	Ward string
	District string `gorm:"index"`
	Country string `gorm:"index"`
	County string `gorm:"index"`
}

// Stringer for PostCodeUnitResponse
func (p PostCodeUnit) String() string {
	return p.ID
}

// PostCodeUnit fetcher
type PostCodeUnitFetcher struct {
	Results []PostCodeUnitResponse
}

// Base URL
func (p *PostCodeUnitFetcher) BaseUrl() string {
	return PostCodeUnitUrl;
}

// Parse JSON results
func (p *PostCodeUnitFetcher) ParseResults(body []byte) (int, error) {
	err := ParseJSON(body,&p.Results)
	return len(p.Results), err
}

func (p *PostCodeUnitFetcher) CreateOrUpdate(db *gorm.DB, index int) error {
	if index >= len(p.Results) {
		return errors.New("Invalid index: " + strconv.Itoa(index))
	} 
	r := p.Results[index]
	pou := PostCodeUnit{}
	db.Where("ID = ?", r.Id).First(&pou)
	unit := &PostCodeUnit{ID: r.Id, Label: FirstOrEmptyXmlValue(r.Labels), Latitude: FirstOrEmptyXmlDataType(r.Latitude),
		Longitude: FirstOrEmptyXmlDataType(r.Longitude), Northing: FirstOrEmptyXmlDataType(r.Northing),
		Easting: FirstOrEmptyXmlDataType(r.Easting), Ward: FirstOrEmptyXmlId(r.Ward), 
		District: FirstOrEmptyXmlId(r.District), Country: FirstOrEmptyXmlId(r.Country), 
		County: FirstOrEmptyXmlId(r.County)}

	if pou.ID == "" {
		err := db.Create(unit).Error

		if err != nil {
			return err
		}
	} else {
		err := db.Save(unit).Error

		if err != nil {
			return err
		}
	}

	return nil
}