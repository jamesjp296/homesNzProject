package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	now := time.Now()

	propertiesSlice, err := GetPropertyDetails()
	if err != nil {
		log.Fatal("Error while getting property details")
	}

	//Test #1 : In the case of duplicates, use the last encountered record
	lastlEntRecordsMap := GetLastEntRecords(propertiesSlice)
	display(lastlEntRecordsMap)

	//Test #2 : In the case of duplicates, use the first encountered record
	firstEntRecordsMap := GetFirstEntRecords(propertiesSlice)
	display(firstEntRecordsMap)

	//Test #3 : Do not insert any of the duplicate records
	nonDuplicateRecordMap := GetNonDuplicates(propertiesSlice)
	display(nonDuplicateRecordMap)

	//Test #4.1: Filter out cheap properties (anything under 400k)
	propertyRecordMap := FilterOutCheapProp(propertiesSlice)
	display(propertyRecordMap)

	//Test #4.2 : Remove the pretentious properties  (AVE, CRES, PL)
	propertyTypeRecordMap := FilterPropType(propertiesSlice)
	display(propertyTypeRecordMap)

	log.Printf("Time taken to process results : %v", time.Now().Sub(now).String())
}

func display(results map[PropertyDetails]int) {

	for key, value := range results {
		fmt.Println(key.StreetAddress, key.Town, key.ValDate, value)
	}

}
