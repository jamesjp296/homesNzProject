package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

/*type PropertyDetails struct {
	StreetAddress string
	Town          string
	ValDate       string
}
*/

func GetPropertyDetails() ([]PropertyValue, error) {
	var textFileRows []string

	file, err := os.Open("properties.txt")
	//file, err := os.Open("testproperties.txt")
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		textFileRows = append(textFileRows, scanner.Text())
	}
	file.Close()

	return getPropertyStruct(textFileRows), nil

}

func getPropertyStruct(textFileRows []string) []PropertyValue {
	var propertyDetailsSlice []PropertyValue
	for i, row := range textFileRows {

		if len(strings.TrimSpace(row)) != 0 && i != 0 {

			column := strings.Split(row, "	")
			var prop PropertyValue

			if len(strings.TrimSpace(column[0])) != 0 {
				prop.ID = column[0]
			}

			if len(strings.TrimSpace(column[1])) != 0 {
				prop.StreetAddress = column[1]
			}
			if len(strings.TrimSpace(column[2])) != 0 {
				prop.Town = column[2]
			}
			if len(strings.TrimSpace(column[3])) != 0 {
				prop.ValuationDate = column[3]
			}
			if len(strings.TrimSpace(column[4])) != 0 {
				var propertyValue, err = strconv.Atoi(column[4])
				if err != nil {
					fmt.Println(err)
				}
				prop.Value = propertyValue
			}

			propertyDetailsSlice = append(propertyDetailsSlice, prop)

		}
	}

	return propertyDetailsSlice
}

//Getting the properties with their last encountered value
func GetLastEntRecords(propertiesSlice []PropertyValue) map[PropertyDetails]int {
	var propertyMap = make(map[PropertyDetails]int)
	for _, prop := range propertiesSlice {
		propMapKey := PropertyDetails{
			StreetAddress: prop.StreetAddress,
			Town:          prop.Town,
			ValDate:       prop.ValuationDate,
		}

		propertyMap[propMapKey] = prop.Value

	}
	return propertyMap
}

func GetFirstEntRecords(propertiesSlice []PropertyValue) map[PropertyDetails]int {
	var propertyMap = make(map[PropertyDetails]int)
	for _, prop := range propertiesSlice {
		propMapKey := PropertyDetails{
			StreetAddress: prop.StreetAddress,
			Town:          prop.Town,
			ValDate:       prop.ValuationDate,
		}

		if _, ok := propertyMap[propMapKey]; !ok {
			propertyMap[propMapKey] = prop.Value
		}

	}
	return propertyMap
}

func GetNonDuplicates(propertiesSlice []PropertyValue) map[PropertyDetails]int {
	var propertyMap = make(map[PropertyDetails]int)
	var dupPropertyMap = make(map[PropertyDetails]int)
	for _, prop := range propertiesSlice {
		propMapKey := PropertyDetails{
			StreetAddress: prop.StreetAddress,
			Town:          prop.Town,
			ValDate:       prop.ValuationDate,
		}

		if _, ok := propertyMap[propMapKey]; ok {
			dupPropertyMap[propMapKey] = prop.Value
		} else {
			propertyMap[propMapKey] = prop.Value
		}

		for key := range dupPropertyMap {
			delete(propertyMap, key)
		}

	}
	return propertyMap
}

func FilterOutCheapProp(propertiesSlice []PropertyValue) map[PropertyDetails]int {
	var propertyMap = make(map[PropertyDetails]int)
	for _, prop := range propertiesSlice {

		if prop.Value > 400000 {

			propMapKey := PropertyDetails{
				StreetAddress: prop.StreetAddress,
				Town:          prop.Town,
				ValDate:       prop.ValuationDate,
			}
			propertyMap[propMapKey] = prop.Value
		}

	}
	return propertyMap
}

func FilterPropType(propertiesSlice []PropertyValue) map[PropertyDetails]int {
	var propertyMap = make(map[PropertyDetails]int)
	for _, prop := range propertiesSlice {

		if !strings.Contains(prop.StreetAddress, "AVE") &&
			!strings.Contains(prop.StreetAddress, "CRES") &&
			!strings.Contains(prop.StreetAddress, "PL") {

			propMapKey := PropertyDetails{
				StreetAddress: prop.StreetAddress,
				Town:          prop.Town,
				ValDate:       prop.ValuationDate,
			}
			propertyMap[propMapKey] = prop.Value
		}

	}
	return propertyMap
}
