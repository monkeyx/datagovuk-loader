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
const EnglandKS2Url = "https://s3-eu-west-1.amazonaws.com/datagovuk/england_ks2.csv"
const EnglandKS4Url = "https://s3-eu-west-1.amazonaws.com/datagovuk/england_ks4.csv"
const EnglandK54Url = "https://s3-eu-west-1.amazonaws.com/datagovuk/england_ks5.csv"

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
	EstablishmentNumber int `gorm:"index"`
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

type SchoolKeyStage2 struct {
	ID int `gorm:"primary_key"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
	LocalAuthorityID int `gorm:"index"`
	EstablishmentNumber int `gorm:"index"`
	PupilsAge11 int // TPUPYEAR
	PublishedEligiblePupilNumber int // TELIG
	EligibleBoys int // BELIG
	EligibleGirls int // GELIG
	PercentageEligibleBoys int // PBELIG (at time of tests)
	PercentageEligibleGirls int // PGELIG (at time of tests)
	KeyStage1Average float64 // TKS1APS
	PupilsLowKeyStage1 int // TKS1EXP_L
	PercentageLowKeyStage1 int // PKS1EXP_L
	PupilsMediumKeyStage1 int // TKS1EXP_M
	PercentageMediumKeyStage1 int // PKS1EXP_M
	PupilsHighKeyStage1 int // TKS1EXP_H
	PercentageHighKeyStage1 int // PKS1EXP_H
	DisadvantagedPupils int // TFSMCLA1A
	PercentageDisadvantaged int // PTFSM6CLA1A
	NotDisadvantagedPupils int // TNOTFSM6CLA1A
	PercentageNotDisadvantaged int // PTNOTFSM6CLA1A
	EnglishSecondLanguage int // TEALGRP2
	PercentageEnglishSecondLanguage int // PTEALGRP2
	NonMobilePupils int // TMOBN
	PercentageNonMobile int // PTMOBN
	SpecialNeeds int // SENELS
	PercentageSpecialNeeds int // PSENELS
	PercentageMathsProgress2Levels int // PT2MATH
	PercentageInMathsProgressMeasured int // COVMATH
	PercentageReadingProgress2Levels int // PT2READ
	PercentageInReadingProgressMeasured int // COVREAD
	PercentageWritingProgress2Levels int // PT2WRITTA
	PercentageInWritingProgressMeasured int // COVWRITTA
	PercentageLevel4Minimum int // PTREADWRITTAMATX
	PercentageLevel48Minimum int // PTREADWRITTAMAT4B
	PercentageLevel5Minimum int // PTREADWRITTAMATAX
	PercentageLevel3Maximum int // PTREADWRITTAMATBX
	AveragePointScore float64 // TAPS
	AverageLevel int // AVGLEVEL
	AverageValueAdded float64 // OVAMEAS
	PercentageInValueAddedMeasure int // VACOV
	OverallConfidenceLower95Limit int // OLCONF
	OverallConfidenceUpper95Limit int // OUCONF
}

func (p SchoolLoader) LoadSchools(db *gorm.DB) (err error) {
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

		if laID, err = strconv.Atoi(r["LA (code)"]); (err == nil && laID != 0) {
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
				log.Println("Unable to persist Local Authority", la.Name, la.ID, "because", err, "Line:", (i + 1))
			}
		} else {
			log.Println("Invalid Local Authority code", err, "Line:", (i + 1))
		}
		id, err := strconv.Atoi(r["URN"])
		if err != nil {
			log.Println("Invalid URN:", r["URN"], "Line:", (i + 1))
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

func (p SchoolLoader) LoadKeyStage2(db *gorm.DB) (err error) {
	body, err := ReadUrl(EnglandKS2Url)

	if err != nil {
		return err
	}

	records, err := ParseCSV(body)

	c := len(records)

	tx := db.Begin()

	for i := 0; i < c; i++ {
		r := records[i]

		// PrintMap(r)
		// break

		id, err := strconv.Atoi(r["URN"])
		if err != nil {
			log.Println("Invalid URN:", r["URN"], "Line:", (i + 1))
			continue
		}
		laID, _ := strconv.Atoi(r["LEA"])
		estNo, _ := strconv.Atoi(r["ESTAB"])
		
		// log.Println("ID:", id)
		PupilsAge11, _ := strconv.Atoi(r["TPUPYEAR"])
		PublishedEligiblePupilNumber, _ := strconv.Atoi(r["TELIG"])
		EligibleBoys, _ := strconv.Atoi(r["BELIG"])
		EligibleGirls, _ := strconv.Atoi(r["GELIG"])
		PercentageEligibleBoys, _ := strconv.Atoi(r["PBELIG"])
		PercentageEligibleGirls, _ := strconv.Atoi(r["PGELIG"])
		KeyStage1Average, _ := strconv.ParseFloat(r["TKS1APS"], 64)
		PupilsLowKeyStage1, _ := strconv.Atoi(r["TKS1EXP_L"])
		PercentageLowKeyStage1, _ := strconv.Atoi(r["PKS1EXP_L"])
		PupilsMediumKeyStage1, _ := strconv.Atoi(r["TKS1EXP_M"])
		PercentageMediumKeyStage1, _ := strconv.Atoi(r["PKS1EXP_M"])
		PupilsHighKeyStage1, _ := strconv.Atoi(r["TKS1EXP_H"])
		PercentageHighKeyStage1, _ := strconv.Atoi(r["PKS1EXP_H"])
		DisadvantagedPupils, _ := strconv.Atoi(r["TFSMCLA1A"])
		PercentageDisadvantaged, _ := strconv.Atoi(r["PTFSM6CLA1A"])
		NotDisadvantagedPupils, _ := strconv.Atoi(r["TNOTFSM6CLA1A"])
		PercentageNotDisadvantaged, _ := strconv.Atoi(r["PTNOTFSM6CLA1A"])
		EnglishSecondLanguage, _ := strconv.Atoi(r["TEALGRP2"])
		PercentageEnglishSecondLanguage, _ := strconv.Atoi(r["PTEALGRP2"])
		NonMobilePupils, _ := strconv.Atoi(r["TMOBN"])
		PercentageNonMobile, _ := strconv.Atoi(r["PTMOBN"])
		SpecialNeeds, _ := strconv.Atoi(r["SENELS"])
		PercentageSpecialNeeds, _ := strconv.Atoi(r["PSENELS"])
		PercentageMathsProgress2Levels, _ := strconv.Atoi(r["PT2MATH"])
		PercentageInMathsProgressMeasured, _ := strconv.Atoi(r["COVMATH"])
		PercentageReadingProgress2Levels, _ := strconv.Atoi(r["PT2READ"])
		PercentageInReadingProgressMeasured, _ := strconv.Atoi(r["COVREAD"])
		PercentageWritingProgress2Levels, _ := strconv.Atoi(r["PT2WRITTA"])
		PercentageInWritingProgressMeasured, _ := strconv.Atoi(r["COVWRITTA"])
		PercentageLevel4Minimum, _ := strconv.Atoi(r["PTREADWRITTAMATX"])
		PercentageLevel48Minimum, _ := strconv.Atoi(r["PTREADWRITTAMAT4B"])
		PercentageLevel5Minimum, _ := strconv.Atoi(r["PTREADWRITTAMATAX"])
		PercentageLevel3Maximum, _ := strconv.Atoi(r["PTREADWRITTAMATBX"])
		AveragePointScore, _ := strconv.ParseFloat(r["TAPS"], 64)
		AverageLevel, _ := strconv.Atoi(r["AVGLEVEL"])
		AverageValueAdded, _ := strconv.ParseFloat(r["OVAMEAS"], 64)
		PercentageInValueAddedMeasure, _ := strconv.Atoi(r["VACOV"])
		OverallConfidenceLower95Limit, _ := strconv.Atoi(r["OLCONF"])
		OverallConfidenceUpper95Limit, _ := strconv.Atoi(r["OUCONF"])

		sch := &SchoolKeyStage2{LocalAuthorityID: laID , 
			EstablishmentNumber: estNo,
			PupilsAge11: PupilsAge11,
			PublishedEligiblePupilNumber: PublishedEligiblePupilNumber,
			EligibleBoys: EligibleBoys,
			EligibleGirls: EligibleGirls,
			PercentageEligibleBoys: PercentageEligibleBoys,
			PercentageEligibleGirls: PercentageEligibleGirls,
			KeyStage1Average: KeyStage1Average,
			PupilsLowKeyStage1: PupilsLowKeyStage1,
			PercentageLowKeyStage1: PercentageLowKeyStage1,
			PupilsMediumKeyStage1: PupilsMediumKeyStage1,
			PercentageMediumKeyStage1: PercentageMediumKeyStage1,
			PupilsHighKeyStage1: PupilsHighKeyStage1,
			PercentageHighKeyStage1: PercentageHighKeyStage1,
			DisadvantagedPupils: DisadvantagedPupils,
			PercentageDisadvantaged: PercentageDisadvantaged,
			NotDisadvantagedPupils: NotDisadvantagedPupils,
			PercentageNotDisadvantaged: PercentageNotDisadvantaged,
			EnglishSecondLanguage: EnglishSecondLanguage,
			PercentageEnglishSecondLanguage: PercentageEnglishSecondLanguage,
			NonMobilePupils: NonMobilePupils,
			PercentageNonMobile: PercentageNonMobile,
			SpecialNeeds: SpecialNeeds,
			PercentageSpecialNeeds: PercentageSpecialNeeds,
			PercentageMathsProgress2Levels: PercentageMathsProgress2Levels,
			PercentageInMathsProgressMeasured: PercentageInMathsProgressMeasured,
			PercentageReadingProgress2Levels: PercentageReadingProgress2Levels,
			PercentageInReadingProgressMeasured: PercentageInReadingProgressMeasured,
			PercentageWritingProgress2Levels: PercentageWritingProgress2Levels,
			PercentageInWritingProgressMeasured: PercentageInWritingProgressMeasured,
			PercentageLevel4Minimum: PercentageLevel4Minimum,
			PercentageLevel48Minimum: PercentageLevel48Minimum,
			PercentageLevel5Minimum: PercentageLevel5Minimum,
			PercentageLevel3Maximum: PercentageLevel3Maximum,
			AveragePointScore: AveragePointScore,
			AverageLevel: AverageLevel,
			AverageValueAdded: AverageValueAdded,
			PercentageInValueAddedMeasure: PercentageInValueAddedMeasure,
			OverallConfidenceLower95Limit: OverallConfidenceLower95Limit,
			OverallConfidenceUpper95Limit: OverallConfidenceUpper95Limit }

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

// Loads post code data
func (p SchoolLoader) Load(db *gorm.DB) (err error) {
	db.AutoMigrate(&LocalAuthority{})
	db.AutoMigrate(&School{})
	db.AutoMigrate(&SchoolKeyStage2{})

	err = p.LoadSchools(db)

	if err != nil {
		return err
	}

	err = p.LoadKeyStage2(db)

	if err != nil {
		return err
	}

	return nil
}