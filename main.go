package main

import (
	"errors"
	"os"
	"os/user"
	"log"
 	"github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/postgres"
    "github.com/monkeyx/datagovuk-loader/dataloaders"
)

// Interface for Data.gov.uk data loaders
type DataLoader interface {
	Load(db *gorm.DB) (error)
}

func sqlConnectionString() (string, error) {
	db_host := os.Getenv("DB_HOST")
	if db_host == "" {
		db_host = "localhost"
	}
	db_user := os.Getenv("DB_USER")
	if db_user == "" {
		user, err := user.Current()
		if err != nil {
			return "", errors.New("DB_USER is not set!")
		}
		db_user = user.Username
	}
	db_password := os.Getenv("DB_PASSWORD") // may be blank
	db_name := os.Getenv("DB_NAME")
	if db_name == "" {
		db_name = "datagovuk"
	}

	dbString := "host=" + db_host + 
		" user=" + db_user + " dbname=" + db_name + " sslmode=disable"

	if db_password == "" {
		return dbString, nil
	}

	return dbString + " password=" + db_password, nil
}

func dataLoader() (DataLoader, error) {
	argsWithoutProg := os.Args[1:]
	if len(argsWithoutProg) < 1 {
		return nil, errors.New("No data loader specified")
	}
	switch argsWithoutProg[0] {
		default: 
			return nil, errors.New("No data loader specified")
		case "postcode":
			return &dataloaders.PostCodeLoader{}, nil
	}
}

func main() {
	log.Println("args: ", os.Args)

	dbString, err := sqlConnectionString()

	if err != nil {
		log.Fatal("Unable to get database connection string:", err)
		return
	}

	log.Println("DB Connection:", dbString)
	db, err := gorm.Open("postgres", dbString)
	defer db.Close()

	if err != nil {
		log.Fatal("Error opening database connection:", err)
		return
	}

	loader, err := dataLoader()

	if err != nil {
		log.Fatal("Error getting data loader:", err)
		return
	}

	err = loader.Load(db)

	if err != nil {
		log.Fatal("Error getting data loader:", err)
		return
	}
}