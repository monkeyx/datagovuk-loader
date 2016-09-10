// Package dataloaders provides various data loaders for Data.gov.uk datasets
package dataloaders

// A typed value structure used in Data.gov.uk
type XmlDataType struct {
	Value string `json:"@value"`
	XmlType string `json:"@type"`
}

// Stringer for XmlDataType
func (p XmlDataType) String() string {
	return p.Value
}

// Gets the first element of an array or if none, returns an empty string
func FirstOrEmptyXmlDataType(array []XmlDataType) string {
	if len(array) > 0 {
		return array[0].Value
	}
	return ""
}

// An identifier structure used in Data.gov.uk
type XmlId struct {
	Id string `json:"@id"`
}

// Stringer for XmlId
func (p XmlId) String() string {
	return p.Id
}

// Gets the first element of an array or if none, returns an empty string
func FirstOrEmptyXmlId(array []XmlId) string {
	if len(array) > 0 {
		return array[0].Id
	}
	return ""
}

// A value structure used in Data.gov.uk
type XmlValue struct {
	Value string `json:"@value"`
}

// Stringer for XmlValue
func (p XmlValue) String() string {
	return p.Value
}

// Gets the first element of an array or if none, returns an empty string
func FirstOrEmptyXmlValue(array []XmlValue) string {
	if len(array) > 0 {
		return array[0].Value
	}
	return ""
}