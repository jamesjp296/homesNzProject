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

	log.Printf("Time taken to process results : %v", time.Now().Sub(now).String())
}

func display(results map[PropertyDetails]int) {

	for key, value := range results {
		fmt.Println(key.StreetAddress, key.Town, key.ValDate, value)
	}

}
