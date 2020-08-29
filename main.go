package main

import (
	"log"
	"time"
)

func main(){
	now := time.Now()

	log.Printf("Time taken to process results : %v", time.Now().Sub(now).String())
}
