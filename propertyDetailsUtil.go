package main

type PropertyDetails struct {
	StreetAddress string
	Town          string
	ValDate       string
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
