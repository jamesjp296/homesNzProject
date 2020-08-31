package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	//Reading the text file
	propertiesSlice, err := GetNewpropertyDetails()
	if err != nil {
		log.Panic("Error while opening file")
	}

	//Last Encountered Record
	lastEntRcdChan := make(chan map[PropertyDetails]int)
	lastRcdChanMsgMap := make(map[PropertyDetails]int)

	//First encountered Record
	firstEntRcdChan := make(chan map[PropertyDetails]int)
	fRChanMsgMap := make(map[PropertyDetails]int)

	// Non duplicate Records
	nonDupRcdChan := make(chan map[PropertyDetails]int)
	nonDupChanMsgMap := make(map[PropertyDetails]int)

	//Last Encountered Record
	go func() {
		GetLastEntRecords(propertiesSlice, lastEntRcdChan)
	}()

	//First encountered Record
	go func() {
		GetFirstEntRecords(propertiesSlice, firstEntRcdChan)
	}()

	// Non duplicate Records
	go func() {
		GetNonDuplicates(propertiesSlice, nonDupRcdChan)
	}()

	//Last Encountered Record
	for lastRcdChanMsg := range lastEntRcdChan {
		lastRcdChanMsgMap = lastRcdChanMsg
	}
	for key, value := range lastRcdChanMsgMap {
		fmt.Println("Last Encontered record : ", key.StreetAddress, key.Town, key.ValDate, value)
	}

	//First encountered Record
	for firstRcdMsgChanMap := range firstEntRcdChan {
		fRChanMsgMap = firstRcdMsgChanMap
	}
	for fRChanMsgKey, fRChanMsgVal := range fRChanMsgMap {
		fmt.Println("First Encontered record : ", fRChanMsgKey.StreetAddress, fRChanMsgKey.Town, fRChanMsgKey.ValDate, fRChanMsgVal)
	}

	// Non duplicate Records
	for nonDupRcdMsgChanMap := range nonDupRcdChan {
		nonDupChanMsgMap = nonDupRcdMsgChanMap
	}

	for key, value := range nonDupChanMsgMap {
		fmt.Println("Non Dup record : ", key.StreetAddress, key.Town, key.ValDate, value)
	}

}

//Getting Non duplicate records
func GetNonDuplicates(propertiesSlice []PropertyValue, chanNonDupRecord chan map[PropertyDetails]int) {
	var nonDupPropertyMap = make(map[PropertyDetails]int)
	var dupPropertyMap = make(map[PropertyDetails]int)

	for _, prop := range propertiesSlice {
		propMapKey := PropertyDetails{
			StreetAddress: prop.StreetAddress,
			Town:          prop.Town,
			ValDate:       prop.ValuationDate,
		}

		if _, ok := nonDupPropertyMap[propMapKey]; ok {
			dupPropertyMap[propMapKey] = prop.Value
		} else {
			nonDupPropertyMap[propMapKey] = prop.Value
		}

		for key := range dupPropertyMap {
			delete(nonDupPropertyMap, key)
		}
	}
	chanNonDupRecord <- nonDupPropertyMap
	close(chanNonDupRecord)
	//return propertyMap
}

//Getting the properties with the First encountered value
func GetFirstEntRecords(propertiesSlice []PropertyValue, firstEntRcdChan chan map[PropertyDetails]int) {
	var firstEntRcdpropertyMap = make(map[PropertyDetails]int)

	for _, firstProp := range propertiesSlice {
		propFirstMapKey := PropertyDetails{
			StreetAddress: firstProp.StreetAddress,
			Town:          firstProp.Town,
			ValDate:       firstProp.ValuationDate,
		}

		if _, ok := firstEntRcdpropertyMap[propFirstMapKey]; !ok {
			firstEntRcdpropertyMap[propFirstMapKey] = firstProp.Value
		}

	}
	firstEntRcdChan <- firstEntRcdpropertyMap
	close(firstEntRcdChan)
	//return propertyMap
}

//Getting the properties with their last encountered value
func GetLastEntRecords(propertiesSlice []PropertyValue, lastEntRcdChan chan map[PropertyDetails]int) {
	var propertyMap = make(map[PropertyDetails]int)
	for _, prop := range propertiesSlice {
		propMapKey := PropertyDetails{
			StreetAddress: prop.StreetAddress,
			Town:          prop.Town,
			ValDate:       prop.ValuationDate,
		}

		propertyMap[propMapKey] = prop.Value

	}
	lastEntRcdChan <- propertyMap
	close(lastEntRcdChan)
	//return propertyMap
}

func GetNewpropertyDetails() ([]PropertyValue, error) {
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
