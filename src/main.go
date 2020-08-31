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

	//Concurrent Jobs
	jobs := make(chan map[PropertyDetails]int, 50)
	results := make(chan map[PropertyDetails]int, 50)
	retFilterRcdPropMap := make(map[PropertyDetails]int)

	splitPropMap := make(map[int]map[PropertyDetails]int)
	splitResultsMap := make(map[int]map[PropertyDetails]int)

	for i := 0; i < 3; i++ {
		splitPropMap[i] = nonDupChanMsgMap
	}

	go worker(jobs, results)
	go worker(jobs, results)

	var numOfJobs = len(splitPropMap)

	for _, todoJobMap := range splitPropMap {
		jobs <- todoJobMap
	}
	close(jobs)

	for i := 0; i < numOfJobs; i++ {
		splitResultsMap[i] = <-results
	}

	for _, filterRcdMsgChanMap := range splitResultsMap {
		retFilterRcdPropMap = filterRcdMsgChanMap

		for key, value := range retFilterRcdPropMap {
			fmt.Println("Filtered record : ", key.StreetAddress, key.Town, key.ValDate, value)
		}

	}

}

// Creating a worker
func worker(jobs <-chan map[PropertyDetails]int, results chan<- map[PropertyDetails]int) {
	for recNonDupMap := range jobs {
		results <- performFilterOperations(recNonDupMap)
	}

}

func performFilterOperations(recNonDupMap map[PropertyDetails]int) map[PropertyDetails]int {
	fltCpPropertyMap := filterOutChpProp(recNonDupMap)
	filterTypePropMap := filterTypeProp(fltCpPropertyMap)

	return filterTypePropMap
}

func filterTypeProp(recNonDupMap map[PropertyDetails]int) map[PropertyDetails]int {

	var propertyMap = make(map[PropertyDetails]int)
	for key, val := range recNonDupMap {

		if !strings.Contains(key.StreetAddress, "AVE") &&
			!strings.Contains(key.StreetAddress, "CRES") &&
			!strings.Contains(key.StreetAddress, "PL") {

			propMapKey := PropertyDetails{
				StreetAddress: key.StreetAddress,
				Town:          key.Town,
				ValDate:       key.ValDate,
			}
			propertyMap[propMapKey] = val
		}

	}
	return propertyMap

}

func filterOutChpProp(recNonDupMap map[PropertyDetails]int) map[PropertyDetails]int {

	//filter out cheap property
	var fltCpPropertyMap = make(map[PropertyDetails]int)

	for propKey, propValue := range recNonDupMap {

		if propValue > 400000 {
			propMapKey := PropertyDetails{
				StreetAddress: propKey.StreetAddress,
				Town:          propKey.Town,
				ValDate:       propKey.ValDate,
			}
			fltCpPropertyMap[propMapKey] = propValue
		}
	}

	return fltCpPropertyMap

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
