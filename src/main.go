package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	textFileName     string = "properties.txt"
	AVE              string = "AVE"
	CRES             string = "CRES"
	PL               string = "PL"
	FilterOutLineNum int32  = 10
)

type count32 int32

var counter count32

type PropertyValue struct {
	ID            string
	StreetAddress string
	Town          string
	ValuationDate string
	Value         int
}

type PropertyDetails struct {
	StreetAddress string
	Town          string
	ValDate       string
}

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
	firstRcdMsgMap := make(map[PropertyDetails]int)

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

	display("Last Encountered record : ", lastRcdChanMsgMap)

	//Receiving First encountered Record from channel
	for firstRcdMsgChanMap := range firstEntRcdChan {
		firstRcdMsgMap = firstRcdMsgChanMap
	}

	display("First Encountered record : ", firstRcdMsgMap)

	//Receiving Non duplicate Records from channel
	for nonDupRcdMsgChanMap := range nonDupRcdChan {
		nonDupChanMsgMap = nonDupRcdMsgChanMap
	}

	display("Non Duplicate record : ", firstRcdMsgMap)

	// Concurrent Jobs
	size := len(nonDupChanMsgMap)
	chanInputs := getInputChan(nonDupChanMsgMap, size)

	chanOperation1 := getfilterOperationChan(chanInputs, size)
	chanOperation2 := getfilterOperationChan(chanInputs, size)

	chanMergeOperation := merge(chanOperation1, chanOperation2)

	for mergOutput := range chanMergeOperation {
		fmt.Println("Filtered records : ", mergOutput.StreetAddress, mergOutput.Town, mergOutput.ValuationDate, mergOutput.Value)
	}

}

func display(displayMsg string, displayRcdMap map[PropertyDetails]int) {
	for key, value := range displayRcdMap {
		fmt.Println(displayMsg, key.StreetAddress, key.Town, key.ValDate, value)
	}
}

func (counter *count32) inc() int32 {
	return atomic.AddInt32((*int32)(counter), 1)
}

func (counter *count32) get() int32 {
	return atomic.LoadInt32((*int32)(counter))
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
	counter.inc()
	if isPropPriceValid(chanInputVal) && isTypePropValid(chanInputVal) && isNotTenthProp() {
		return true
	}

	return false
}

func isPropPriceValid(chanInputVal PropertyValue) bool {

	if chanInputVal.Value > 400000 {
		return true
	}

	return false
}

func isTypePropValid(chanInputVal PropertyValue) bool {

	if !strings.Contains(chanInputVal.StreetAddress, AVE) &&
		!strings.Contains(chanInputVal.StreetAddress, CRES) &&
		!strings.Contains(chanInputVal.StreetAddress, PL) {
		return true
	}
	return false
}

func isNotTenthProp() bool {

	if counter.get()%FilterOutLineNum == 0 {
		return false
	}

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
