package model

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Property struct {
	ID            string
	StreetAddress string
	Town          string
	ValuationDate string
	Value         int
}


func GetPropertyDetails()([]Property, error){
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

	return getPropertyStruct(textFileRows),nil

}

func getPropertyStruct(textFileRows []string) []Property {
	var propertyDetailsSlice []Property
	for i, row := range textFileRows {

		if len(strings.TrimSpace(row)) != 0 && i != 0 {

			column := strings.Split(row, "	")
			var prop Property

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