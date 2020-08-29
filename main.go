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

	fmt.Println("The length of the properites Slice ", len(propertiesSlice))

	/*-------------------Test #1 : In the case of duplicates, use the last encountered record ----------------------*/
	fmt.Println("-------Test #1 : In the case of duplicates, use the last encountered record-------------")
	laslEntRecordsMap := GetLastEntRecords(propertiesSlice)
	fmt.Println("The length of last encountered records: ", len(laslEntRecordsMap))
	display(laslEntRecordsMap)

	/*-------------------Test #2 : In the case of duplicates, use the first encountered record ----------------------*/
	fmt.Println("-------------Test #2 : In the case of duplicates, use the first encountered record-------------")
	firstEntRecordsMap := GetFirstEntRecords(propertiesSlice)
	fmt.Println("The length of first encountered records: ", len(firstEntRecordsMap))
	display(firstEntRecordsMap)

	/*-------------------Test #3 : Do not insert any of the duplicate records ----------------------*/
	fmt.Println("-------------Test #3 : Do not insert any of the duplicate records-------------")
	nonDuplicateRecordMap := GetNonDuplicates(propertiesSlice)
	display(nonDuplicateRecordMap)
	fmt.Println("-------------Non duplicate records: ", len(nonDuplicateRecordMap), "-------------")

	/*-------------------Test #4.1: Filter out cheap properties (anything under 400k) ----------------------*/
	fmt.Println("-------------Test #4.1 : Filter out cheap properties (anything under 400k)-------------")
	propertyRecordMap := FilterOutCheapProp(propertiesSlice)
	fmt.Println("FilterOutCheapProp records: ", len(propertyRecordMap))
	display(propertyRecordMap)
	/*-------------------Test #4.2 : Remove the pretentious properties  (AVE, CRES, PL)  ----------------------*/
	fmt.Println("Test #4.2 : Remove the pretentious properties  (AVE, CRES, PL)")
	propertyTypeRecordMap := FilterPropType(propertiesSlice)
	fmt.Println("The length of Non duplicate records: ", len(propertyTypeRecordMap))
	display(propertyTypeRecordMap)

	log.Printf("Time taken to process results : %v", time.Now().Sub(now).String())
}

func display(results map[PropertyDetails]int) {

	for key, value := range results {
		fmt.Println(key.StreetAddress, key.Town, key.ValDate, value)
	}

}
