package dataloaders

import (
	"log"
	"strconv"
	"time"
	"github.com/jinzhu/gorm"
)

// SchoolLoader is a data loader for Dept of Education EduBase CSV format files
type SchoolLoader struct {}

const EduBaseUrl = "https://s3-eu-west-1.amazonaws.com/datagovuk/edubasealldata.csv"

// Local authority type
type LocalAuthority struct {
	ID int `gorm:"primary_key"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
	Name string
}

// School type
type School struct {
	ID int `gorm:"primary_key"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
	LocalAuthorityID int `gorm:"index"`
	EstablishmentNumber int
	EstablishmentName string
	EstablishmentType string
	EstablishmentStatus string
	EstablishmentReasonOpened string
	OpenDate time.Time
	CloseDate time.Time
	PhaseOfEducation string
	StatutoryLowAge int
	StatutoryHighAge int
	Boarders string
	OfficialSixthForm string
	Gender string
	ReligiousCharacter string
	Diocese string
	AdmissionsPolicy string
	SchoolCapacity int
	SpecialClasses string
	FurtherEducationType string
	OfstedSpecialMeasures string
	LastChangedDate time.Time
	Street string
	Locality string
	Address3 string
	Town string
	County string
	Postcode string
	SchoolWebsite string
	TelephoneNum string
	HeadTitle string
	HeadFirstName string
	HeadLastName string
	HeadHonours string
	HeadPreferredJobTitle string
	GOR string
	AdministrativeWard string
	ParliamentaryConstituency string
	UrbanRural string
	GSSLACode string
	Easting int
	Northing int
	MSOA string
	LSOA string
	BoardingEstablishment string
	PreviousLA int
	PreviousLAName string
	PreviousEstablishmentNumber int
}

// Loads post code data
func (p SchoolLoader) Load(db *gorm.DB) (err error) {
	db.AutoMigrate(&LocalAuthority{})
	db.AutoMigrate(&School{})

	body, err := ReadUrl(EduBaseUrl)

	if err != nil {
		return err
	}

	records, err := ParseCSV(body)

	c := len(records)

	tx := db.Begin()

	for i := 0; i < c; i++ {
		r := records[i]

		// PrintMap(r)

		var laID = 0

		if laID, err = strconv.Atoi(r["LA (code)"]); err == nil {
			la := &LocalAuthority{}
			db.Where("ID = ?", laID).First(la)
			la.Name = r["LA (name)"]
			if la.ID == 0 {
				la.ID = laID
				err = db.Create(la).Error
			} else {
				err = db.Save(la).Error
			}
			if err != nil {
				tx.Rollback()
				return err
			}
		} else {
			log.Println("Invalid Local Authority code", err)
		}
		id, err := strconv.Atoi(r["URN"])
		if err != nil {
			log.Println("Invalid URN:", r["URN"])
			continue
		}
		// log.Println("ID:", id)
		estNo, _ := strconv.Atoi(r["EstablishmentNumber"])
		openDate, _ := ParseSimpleDate(r["OpenDate"])
		closeDate, _ := ParseSimpleDate(r["CloseDate"])
		lowAge, _ := strconv.Atoi(r["StatutoryLowAge"])
		highAge, _ := strconv.Atoi(r["StatutoryHighAge"])
		schoolCapacity, _ := strconv.Atoi(r["SchoolCapacity"])
		lastChangedDate, _ := ParseSimpleDate(r["LastChangedDate"])
		easting, _ := strconv.Atoi(r["Easting"])
		northing, _ := strconv.Atoi(r["Northing"])
		previousLA, _ := strconv.Atoi(r["PreviousLA (code)"])
		previousEstNo, _ := strconv.Atoi(r["PreviousEstablishmentNumber"])

		sch := &School{LocalAuthorityID: laID , 
			EstablishmentNumber: estNo, EstablishmentName: r["EstablishmentName"],
			EstablishmentType: r["TypeOfEstablishment (name)"], EstablishmentStatus: r["EstablishmentStatus (name)"],
			EstablishmentReasonOpened: r["ReasonEstablishmentOpened (name)"],
			OpenDate: openDate, CloseDate: closeDate, PhaseOfEducation: r["PhaseOfEducation (name)"],
			StatutoryLowAge: lowAge, StatutoryHighAge: highAge, Boarders: r["Boarders (name)"],
			OfficialSixthForm: r["OfficialSixthForm (name)"], Gender: r["Gender (name)"],
			ReligiousCharacter: r["ReligiousCharacter (name)"], Diocese: r["Diocese (name)"],
			AdmissionsPolicy: r["AdmissionsPolicy (name)"], SchoolCapacity: schoolCapacity,
			SpecialClasses: r["SpecialClasses (name)"], FurtherEducationType: r["FurtherEducationType (name)"],
			OfstedSpecialMeasures: r["OfstedSpecialMeasures (name)"], LastChangedDate: lastChangedDate,
			Street: r["Street"], Locality: r["Locality"], Address3: r["Address3"], Town: r["Town"],
			County: r["County (name)"], Postcode: r["Postcode"], SchoolWebsite: r["SchoolWebsite"],
			TelephoneNum: r["TelephoneNum"], HeadTitle: r["HeadTitle (name)"], HeadFirstName: r["HeadFirstName"],
			HeadLastName: r["HeadLastName"], HeadHonours: r["HeadHonours"], HeadPreferredJobTitle: r["HeadPreferredJobTitle"],
			GOR: r["GOR (name)"], AdministrativeWard: r["AdministrativeWard (name)"], 
			ParliamentaryConstituency: r["ParliamentaryConstituency (name)"], UrbanRural: r["UrbanRural (name)"],
			GSSLACode: r["GSSLACode (name)"], Easting: easting, Northing: northing, MSOA: r["MSOA (name)"],
			LSOA: r["LSOA (name)"], BoardingEstablishment: r["BoardingEstablishment (name)"],
			PreviousLA: previousLA, PreviousLAName: r["PreviousLA (code)"], 
			PreviousEstablishmentNumber: previousEstNo }

		db.Where("ID = ?", id).First(sch)

		if sch.ID == 0 {
			sch.ID = id
			err := db.Create(sch).Error 
			if err != nil {
				return err
			}
		} else {
			err := db.Save(sch).Error 
			if err != nil {
				return err
			}
		}
	}

	tx.Commit()
	return nil
}