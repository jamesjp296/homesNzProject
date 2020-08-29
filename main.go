package main

import (
	"fmt"
	"github.com/jamesjp296/homesNzProject/model"
	"log"
	"time"
)

func main(){
	now := time.Now()

	propertiesSlice, err :=model.GetPropertyDetails()
	if err != nil {
		log.Fatal("Error while getting property details")
	}

	fmt.Println("The length of the properites Slice ", len(propertiesSlice))
	log.Printf("Time taken to process results : %v", time.Now().Sub(now).String())
}
