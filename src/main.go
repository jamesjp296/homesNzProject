package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	textFileName     string = "testproperties.txt"
	AVE              string = "AVE"
	CRES             string = "CRES"
	PL               string = "PL"
	FilterOutLineNum int    = 10
)

func main() {

	//Reading the text file
	propertiesSlice, err := GetPropertyDetails()
	if err != nil {
		log.Panic("Error while opening file")
	}

	//Creating channels to get Last Encountered Record
	lastEntRcdChan := make(chan map[PropertyDetails]int)
	lastRcdChanMsgMap := make(map[PropertyDetails]int)

	//Creating channels to get First encountered Record
	firstEntRcdChan := make(chan map[PropertyDetails]int)
	fRChanMsgMap := make(map[PropertyDetails]int)

	//Creating channels to get Non duplicate Records
	nonDupRcdChan := make(chan map[PropertyDetails]int)
	nonDupChanMsgMap := make(map[PropertyDetails]int)

	//go routine to get Last Encountered Record
	go func() {
		GetLastEntRecords(propertiesSlice, lastEntRcdChan)
	}()

	//go routine to get First encountered Record
	go func() {
		GetFirstEntRecords(propertiesSlice, firstEntRcdChan)
	}()

	//go routine to get Non duplicate Records
	go func() {
		GetNonDuplicates(propertiesSlice, nonDupRcdChan)
	}()

	//Receiving Last Encountered Record from channel
	for lastRcdChanMsg := range lastEntRcdChan {
		lastRcdChanMsgMap = lastRcdChanMsg
	}
	for key, value := range lastRcdChanMsgMap {
		fmt.Println("Last Encountered record : ", key.StreetAddress, key.Town, key.ValDate, value)
	}

	//Receiving First encountered Record from channel
	for firstRcdMsgChanMap := range firstEntRcdChan {
		fRChanMsgMap = firstRcdMsgChanMap
	}
	for fRChanMsgKey, fRChanMsgVal := range fRChanMsgMap {
		fmt.Println("First Encountered record : ", fRChanMsgKey.StreetAddress, fRChanMsgKey.Town, fRChanMsgKey.ValDate, fRChanMsgVal)
	}

	//Receiving Non duplicate Records from channel
	for nonDupRcdMsgChanMap := range nonDupRcdChan {
		nonDupChanMsgMap = nonDupRcdMsgChanMap
	}

	for key, value := range nonDupChanMsgMap {
		fmt.Println("Non Dup record : ", key.StreetAddress, key.Town, key.ValDate, value)
	}

	// Concurrent Jobs
	size := len(nonDupChanMsgMap)
	chanInputs := getInputChan(nonDupChanMsgMap, size)

	chanOperation1 := getfilterOperationChan(chanInputs, size)
	chanOperation2 := getfilterOperationChan(chanInputs, size)

	chanMergeOperation := merge(chanOperation1, chanOperation2)

	for mergOutput := range chanMergeOperation {
		fmt.Println("Merge output", mergOutput)
	}

	/*
		//Concurrent Jobs
		jobs := make(chan map[PropertyDetails]int, 50)
		results := make(chan map[PropertyDetails]int, 50)
		retFilterRcdPropMap := make(map[PropertyDetails]int)

		splitPropMap := make(map[int]map[PropertyDetails]int)
		splitResultsMap := make(map[int]map[PropertyDetails]int)

		for i := 0; i < 3; i++ {
			splitPropMap[i] = nonDupChanMsgMap
		}

		// Performing go routine on chunks
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

		} */

}

//getInputChan()
func getInputChan(nonDupPropMap map[PropertyDetails]int, size int) <-chan PropertyValue {

	input := make(chan PropertyValue, size)

	go func() {
		for propKey, propVal := range nonDupPropMap {
			propValStr := PropertyValue{
				StreetAddress: propKey.StreetAddress,
				Town:          propKey.Town,
				ValuationDate: propKey.ValDate,
				Value:         propVal,
			}
			input <- propValStr
		}
		close(input)
	}()
	return input
}

//getfilterOperationChan(chanInputs)
func getfilterOperationChan(chanInputs <-chan PropertyValue, size int) <-chan PropertyValue {
	output := make(chan PropertyValue, size)

	go func() {
		for chanInputVal := range chanInputs {
			if isValidRecord(chanInputVal) {
				output <- chanInputVal
			}
		}
		close(output)
	}()

	return output
}

func isValidRecord(chanInputVal PropertyValue) bool {
	return true
}

func merge(outputsChan ...<-chan PropertyValue) <-chan PropertyValue {

	var wg sync.WaitGroup

	merged := make(chan PropertyValue, 100)

	wg.Add(len(outputsChan))

	mergeOutput := func(optsChan <-chan PropertyValue) {
		for propValStr := range optsChan {
			merged <- propValStr
		}
		wg.Done()
	}

	for _, optsChan := range outputsChan {
		go mergeOutput(optsChan)
	}

	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
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
	filterTenthPropMap := filterTenthProp(filterTypePropMap)

	return filterTenthPropMap
}

func filterTenthProp(recNonDupMap map[PropertyDetails]int) map[PropertyDetails]int {

	var propertyMap = make(map[PropertyDetails]int)
	i := 1
	for key, val := range recNonDupMap {

		if i != FilterOutLineNum {
			propMapKey := PropertyDetails{
				StreetAddress: key.StreetAddress,
				Town:          key.Town,
				ValDate:       key.ValDate,
			}
			propertyMap[propMapKey] = val
		} else {
			i = 0
		}

		i++

	}
	return propertyMap

}

func filterTypeProp(recNonDupMap map[PropertyDetails]int) map[PropertyDetails]int {

	var propertyMap = make(map[PropertyDetails]int)
	for key, val := range recNonDupMap {

		if !strings.Contains(key.StreetAddress, AVE) &&
			!strings.Contains(key.StreetAddress, CRES) &&
			!strings.Contains(key.StreetAddress, PL) {

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

}

func GetPropertyDetails() ([]PropertyValue, error) {
	var textFileRows []string

	file, err := os.Open(textFileName)
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
